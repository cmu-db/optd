use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    sync::Arc,
};

use anyhow::Result;

use crate::{
    cost::CostModel,
    optimizer::Optimizer,
    property::{PropertyBuilder, PropertyBuilderAny},
    rel_node::{RelNodeRef, RelNodeTyp},
    rules::Rule,
};

use super::{
    memo::{GroupInfo, RelMemoNodeRef},
    tasks::OptimizeGroupTask,
    Memo, Task,
};

pub type RuleId = usize;

#[derive(Default, Clone, Debug)]
pub struct OptimizerContext {
    pub upper_bound: Option<f64>,
    pub budget_used: bool,
}

pub struct CascadesOptimizer<T: RelNodeTyp> {
    memo: Memo<T>,
    pub(super) tasks: VecDeque<Box<dyn Task<T>>>,
    explored_group: HashSet<GroupId>,
    fired_rules: HashMap<ExprId, HashSet<RuleId>>,
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    cost: Arc<dyn CostModel<T>>,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    pub ctx: OptimizerContext,
}

/// `RelNode` only contains the representation of the plan nodes. Sometimes, we need more context, i.e., group id and
/// expr id, during the optimization phase. All these information are collected in this struct.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct RelNodeContext {
    pub group_id: GroupId,
    pub expr_id: ExprId,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct GroupId(pub(super) usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct ExprId(pub usize);

impl Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.0)
    }
}

impl Display for ExprId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: RelNodeTyp> CascadesOptimizer<T> {
    pub fn new(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        cost: Box<dyn CostModel<T>>,
        property_builders: Vec<Box<dyn PropertyBuilderAny<T>>>,
    ) -> Self {
        let tasks = VecDeque::new();
        let property_builders: Arc<[_]> = property_builders.into();
        let memo = Memo::new(property_builders.clone());
        Self {
            memo,
            tasks,
            explored_group: HashSet::new(),
            fired_rules: HashMap::new(),
            rules: rules.into(),
            cost: cost.into(),
            ctx: OptimizerContext::default(),
            property_builders,
        }
    }

    pub fn cost(&self) -> Arc<dyn CostModel<T>> {
        self.cost.clone()
    }

    pub(super) fn rules(&self) -> Arc<[Arc<dyn Rule<T, Self>>]> {
        self.rules.clone()
    }

    pub fn dump(&self, group_id: Option<GroupId>) {
        if let Some(group_id) = group_id {
            fn dump_inner<T: RelNodeTyp>(this: &CascadesOptimizer<T>, group_id: GroupId) {
                if let Some(ref winner) = this.memo.get_group_info(group_id).winner {
                    let expr = this.memo.get_expr_memoed(winner.expr_id);
                    assert!(!winner.impossible);
                    if winner.cost.0[1] == 1.0 {
                        return;
                    }
                    println!(
                        "group_id={} winner={} cost={} {}",
                        group_id,
                        winner.expr_id,
                        this.cost.explain(&winner.cost),
                        expr
                    );
                    for child in &expr.children {
                        dump_inner(this, *child);
                    }
                }
            }
            dump_inner(self, group_id);
            return;
        }
        for group_id in self.memo.get_all_group_ids() {
            let winner = if let Some(ref winner) = self.memo.get_group_info(group_id).winner {
                if winner.impossible {
                    "winner=<impossible>".to_string()
                } else {
                    format!(
                        "winner={} cost={} {}",
                        winner.expr_id,
                        self.cost.explain(&winner.cost),
                        self.memo.get_expr_memoed(winner.expr_id)
                    )
                }
            } else {
                "winner=None".to_string()
            };
            println!("group_id={} {}", group_id, winner);
            let group = self.memo.get_group(group_id);
            for (id, property) in self.property_builders.iter().enumerate() {
                println!(
                    "  {}={}",
                    property.property_name(),
                    property.display(group.properties[id].as_ref())
                )
            }
            for expr_id in self.memo.get_all_exprs_in_group(group_id) {
                let memo_node = self.memo.get_expr_memoed(expr_id);
                println!("  expr_id={} | {}", expr_id, memo_node);
                let bindings = self
                    .memo
                    .get_all_expr_bindings(expr_id, false, true, Some(1));
                for binding in bindings {
                    println!("    {}", binding);
                }
            }
        }
    }

    fn fire_optimize_tasks(&mut self, group_id: GroupId) -> Result<()> {
        self.memo.clear_winner();
        self.tasks
            .push_back(Box::new(OptimizeGroupTask::new(group_id)));
        // get the task from the stack
        self.ctx.budget_used = false;
        let mut num_tasks = 0;
        while let Some(task) = self.tasks.pop_back() {
            let new_tasks = task.execute(self)?;
            self.tasks.extend(new_tasks);
            num_tasks += 1;
            if num_tasks == (1 << 16) {
                println!("budget used, not applying logical rules any more");
                self.ctx.budget_used = true;
            }
        }
        Ok(())
    }

    fn optimize_inner(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        let (group_id, _) = self.add_group_expr(root_rel, None);
        self.fire_optimize_tasks(group_id)?;
        self.memo.get_best_group_binding(group_id, &mut |x, _| x)
    }

    pub fn optimize_with_on_produce_callback(
        &mut self,
        root_rel: RelNodeRef<T>,
        mut on_produce: impl FnMut(RelNodeRef<T>, GroupId) -> RelNodeRef<T>,
    ) -> Result<(GroupId, RelNodeRef<T>)> {
        let (group_id, _) = self.add_group_expr(root_rel, None);
        self.fire_optimize_tasks(group_id)?;
        Ok((
            group_id,
            self.memo
                .get_best_group_binding(group_id, &mut on_produce)?,
        ))
    }

    pub fn resolve_group_id(&self, root_rel: RelNodeRef<T>) -> GroupId {
        if let Some(group_id) = T::extract_group(&root_rel.typ) {
            return group_id;
        }
        let (group_id, _) = self.get_expr_info(root_rel);
        group_id
    }

    pub(super) fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.memo.get_all_exprs_in_group(group_id)
    }

    pub(super) fn get_expr_info(&self, expr: RelNodeRef<T>) -> (GroupId, ExprId) {
        self.memo.get_expr_info(expr)
    }

    pub(super) fn add_group_expr(
        &mut self,
        expr: RelNodeRef<T>,
        group_id: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        self.memo.add_new_group_expr(expr, group_id)
    }

    pub(super) fn get_group_info(&self, group_id: GroupId) -> GroupInfo {
        self.memo.get_group_info(group_id)
    }

    pub(super) fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
        self.memo.update_group_info(group_id, group_info)
    }

    pub fn get_property_by_group<P: PropertyBuilder<T>>(
        &self,
        group_id: GroupId,
        idx: usize,
    ) -> P::Prop {
        self.memo.get_group(group_id).properties[idx]
            .downcast_ref::<P::Prop>()
            .unwrap()
            .clone()
    }

    pub(super) fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        self.memo.get_group_id(expr_id)
    }

    pub(super) fn get_expr_memoed(&self, expr_id: ExprId) -> RelMemoNodeRef<T> {
        self.memo.get_expr_memoed(expr_id)
    }

    pub(super) fn get_all_expr_bindings(
        &self,
        expr_id: ExprId,
        level: Option<usize>,
    ) -> Vec<RelNodeRef<T>> {
        self.memo
            .get_all_expr_bindings(expr_id, false, false, level)
    }

    pub fn get_all_group_physical_bindings(&self, group_id: GroupId) -> Vec<RelNodeRef<T>> {
        self.memo
            .get_all_group_bindings(group_id, true, true, Some(10))
    }

    pub(super) fn is_group_explored(&self, group_id: GroupId) -> bool {
        self.explored_group.contains(&group_id)
    }

    pub(super) fn mark_group_explored(&mut self, group_id: GroupId) {
        self.explored_group.insert(group_id);
    }

    pub(super) fn is_rule_fired(&self, group_expr_id: ExprId, rule_id: RuleId) -> bool {
        self.fired_rules
            .get(&group_expr_id)
            .map(|rules| rules.contains(&rule_id))
            .unwrap_or(false)
    }

    pub(super) fn mark_rule_fired(&mut self, group_expr_id: ExprId, rule_id: RuleId) {
        self.fired_rules
            .entry(group_expr_id)
            .or_default()
            .insert(rule_id);
    }
}

impl<T: RelNodeTyp> Optimizer<T> for CascadesOptimizer<T> {
    fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: PropertyBuilder<T>>(&self, root_rel: RelNodeRef<T>, idx: usize) -> P::Prop {
        self.get_property_by_group::<P>(self.resolve_group_id(root_rel), idx)
    }
}
