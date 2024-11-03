use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    sync::Arc,
};

use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::memo::Winner,
    cost::CostModel,
    optimizer::Optimizer,
    property::{PropertyBuilder, PropertyBuilderAny},
    rel_node::{RelNodeMeta, RelNodeMetaMap, RelNodeRef, RelNodeTyp},
    rules::RuleWrapper,
};

use super::{
    memo::{GroupInfo, Memo, RelMemoNodeRef},
    tasks::OptimizeGroupTask,
    NaiveMemo, Task,
};

pub type RuleId = usize;

#[derive(Default, Clone, Debug)]
pub struct OptimizerContext {
    pub budget_used: bool,
    pub rules_applied: usize,
}

#[derive(Default, Clone, Debug)]
pub struct OptimizerProperties {
    pub panic_on_budget: bool,
    /// If the number of rules applied exceeds this number, we stop applying logical rules.
    pub partial_explore_iter: Option<usize>,
    /// Plan space can be expanded by this number of times before we stop applying logical rules.
    pub partial_explore_space: Option<usize>,
}

pub struct CascadesOptimizer<T: RelNodeTyp, M: Memo<T> = NaiveMemo<T>> {
    memo: M,
    pub(super) tasks: VecDeque<Box<dyn Task<T, M>>>,
    explored_group: HashSet<GroupId>,
    explored_expr: HashSet<ExprId>,
    fired_rules: HashMap<ExprId, HashSet<RuleId>>,
    rules: Arc<[Arc<RuleWrapper<T, Self>>]>,
    disabled_rules: HashSet<usize>,
    cost: Arc<dyn CostModel<T, M>>,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    pub ctx: OptimizerContext,
    pub prop: OptimizerProperties,
}

/// `RelNode` only contains the representation of the plan nodes. Sometimes, we need more context, i.e., group id and
/// expr id, during the optimization phase. All these information are collected in this struct.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct RelNodeContext {
    pub group_id: GroupId,
    pub expr_id: ExprId,
    pub children_group_ids: Vec<GroupId>,
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

impl<T: RelNodeTyp> CascadesOptimizer<T, NaiveMemo<T>> {
    pub fn new(
        rules: Vec<Arc<RuleWrapper<T, Self>>>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        property_builders: Vec<Box<dyn PropertyBuilderAny<T>>>,
    ) -> Self {
        Self::new_with_prop(rules, cost, property_builders, Default::default())
    }

    pub fn new_with_prop(
        rules: Vec<Arc<RuleWrapper<T, Self>>>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        property_builders: Vec<Box<dyn PropertyBuilderAny<T>>>,
        prop: OptimizerProperties,
    ) -> Self {
        let tasks = VecDeque::new();
        let property_builders: Arc<[_]> = property_builders.into();
        let memo = NaiveMemo::new(property_builders.clone());
        Self {
            memo,
            tasks,
            explored_group: HashSet::new(),
            explored_expr: HashSet::new(),
            fired_rules: HashMap::new(),
            rules: rules.into(),
            cost: cost.into(),
            ctx: OptimizerContext::default(),
            property_builders,
            prop,
            disabled_rules: HashSet::new(),
        }
    }

    /// Clear the memo table and all optimizer states.
    pub fn step_clear(&mut self) {
        self.memo = NaiveMemo::new(self.property_builders.clone());
        self.fired_rules.clear();
        self.explored_group.clear();
        self.explored_expr.clear();
    }

    /// Clear the winner so that the optimizer can continue to explore the group.
    pub fn step_clear_winner(&mut self) {
        self.memo.clear_winner();
        self.explored_expr.clear();
    }
}

impl<T: RelNodeTyp, M: Memo<T>> CascadesOptimizer<T, M> {
    pub fn panic_on_explore_limit(&mut self, enabled: bool) {
        self.prop.panic_on_budget = enabled;
    }

    pub fn cost(&self) -> Arc<dyn CostModel<T, M>> {
        self.cost.clone()
    }

    pub fn rules(&self) -> Arc<[Arc<RuleWrapper<T, Self>>]> {
        self.rules.clone()
    }

    pub fn disable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.insert(rule_id);
    }

    pub fn enable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.remove(&rule_id);
    }

    pub fn is_rule_disabled(&self, rule_id: usize) -> bool {
        self.disabled_rules.contains(&rule_id)
    }

    pub fn dump(&self) {
        for group_id in self.memo.get_all_group_ids() {
            let winner_str = match &self.memo.get_group_info(group_id).winner {
                Winner::Impossible => "winner=<impossible>".to_string(),
                Winner::Unknown => "winner=<unknown>".to_string(),
                Winner::Full(winner) => {
                    let expr = self.memo.get_expr_memoed(winner.expr_id);
                    format!(
                        "winner={} weighted_cost={} cost={} stat={} | {}",
                        winner.expr_id,
                        winner.total_weighted_cost,
                        self.cost.explain_cost(&winner.total_cost),
                        self.cost.explain_statistics(&winner.statistics),
                        expr
                    )
                }
            };
            println!("group_id={} {}", group_id, winner_str);
            let group = self.memo.get_group(group_id);
            for (id, property) in self.property_builders.iter().enumerate() {
                println!(
                    "  {}={}",
                    property.property_name(),
                    property.display(group.properties[id].as_ref())
                )
            }
            if let Some(predicate_binding) = self.memo.try_get_predicate_binding(group_id) {
                println!("  predicate={}", predicate_binding);
            }
            for expr_id in self.memo.get_all_exprs_in_group(group_id) {
                let memo_node = self.memo.get_expr_memoed(expr_id);
                println!("  expr_id={} | {}", expr_id, memo_node);
            }
        }
    }

    /// Optimize a `RelNode`.
    pub fn step_optimize_rel(&mut self, root_rel: RelNodeRef<T>) -> Result<GroupId> {
        let (group_id, _) = self.add_new_expr(root_rel);
        self.fire_optimize_tasks(group_id)?;
        Ok(group_id)
    }

    /// Gets the group binding.
    pub fn step_get_optimize_rel(
        &self,
        group_id: GroupId,
        meta: &mut Option<RelNodeMetaMap>,
    ) -> Result<RelNodeRef<T>> {
        let res = self
            .memo
            .get_best_group_binding(group_id, |node, group_id, info| {
                if let Some(meta) = meta {
                    let node = node.as_ref() as *const _ as usize;
                    let node_meta = RelNodeMeta::new(
                        group_id,
                        info.total_weighted_cost,
                        info.total_cost.clone(),
                        info.statistics.clone(),
                        self.cost.explain_cost(&info.total_cost),
                        self.cost.explain_statistics(&info.statistics),
                    );
                    meta.insert(node, node_meta);
                }
            });
        if res.is_err() && cfg!(debug_assertions) {
            self.dump();
        }
        res
    }

    fn fire_optimize_tasks(&mut self, group_id: GroupId) -> Result<()> {
        trace!(event = "fire_optimize_tasks", root_group_id = %group_id);
        self.tasks
            .push_back(Box::new(OptimizeGroupTask::new(group_id)));
        // get the task from the stack
        self.ctx.budget_used = false;
        let plan_space_begin = self.memo.estimated_plan_space();
        let mut iter = 0;
        while let Some(task) = self.tasks.pop_back() {
            let new_tasks = task.execute(self)?;
            self.tasks.extend(new_tasks);
            iter += 1;
            if !self.ctx.budget_used {
                let plan_space = self.memo.estimated_plan_space();
                if let Some(partial_explore_space) = self.prop.partial_explore_space {
                    if plan_space - plan_space_begin > partial_explore_space {
                        println!(
                            "plan space size budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.ctx.budget_used = true;
                        if self.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                } else if let Some(partial_explore_iter) = self.prop.partial_explore_iter {
                    if iter >= partial_explore_iter {
                        println!(
                            "plan explore iter budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.ctx.budget_used = true;
                        if self.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn optimize_inner(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        let (group_id, _) = self.add_new_expr(root_rel);
        self.fire_optimize_tasks(group_id)?;
        self.memo.get_best_group_binding(group_id, |_, _, _| {})
    }

    pub fn resolve_group_id(&self, root_rel: RelNodeRef<T>) -> GroupId {
        if let Some(group_id) = T::extract_group(&root_rel.typ) {
            return group_id;
        }
        panic!("This function is deprecated -- you should only pass group id instead of a full expression to this function.")
    }

    pub(super) fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.memo.get_all_exprs_in_group(group_id)
    }

    pub fn add_new_expr(&mut self, rel_node: RelNodeRef<T>) -> (GroupId, ExprId) {
        self.memo.add_new_expr(rel_node)
    }

    pub fn add_expr_to_group(
        &mut self,
        rel_node: RelNodeRef<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        self.memo.add_expr_to_group(rel_node, group_id)
    }

    pub(super) fn get_group_info(&self, group_id: GroupId) -> &GroupInfo {
        self.memo.get_group_info(group_id)
    }

    pub(super) fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
        self.memo.update_group_info(group_id, group_info)
    }

    /// Get the properties of a Cascades group
    /// P is the type of the property you expect
    /// idx is the idx of the property you want. The order of properties is defined
    ///   by the property_builders parameter in CascadesOptimizer::new()
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

    pub fn get_predicate_binding(&self, group_id: GroupId) -> Option<RelNodeRef<T>> {
        self.memo.get_predicate_binding(group_id)
    }

    pub(super) fn is_group_explored(&self, group_id: GroupId) -> bool {
        self.explored_group.contains(&group_id)
    }

    pub(super) fn mark_group_explored(&mut self, group_id: GroupId) {
        self.explored_group.insert(group_id);
    }

    pub(super) fn is_expr_explored(&self, expr_id: ExprId) -> bool {
        self.explored_expr.contains(&expr_id)
    }

    pub(super) fn mark_expr_explored(&mut self, expr_id: ExprId) {
        self.explored_expr.insert(expr_id);
    }

    pub(super) fn unmark_expr_explored(&mut self, expr_id: ExprId) {
        self.explored_expr.remove(&expr_id);
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

    pub fn memo(&self) -> &M {
        &self.memo
    }
}

impl<T: RelNodeTyp, M: Memo<T>> Optimizer<T> for CascadesOptimizer<T, M> {
    fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: PropertyBuilder<T>>(&self, root_rel: RelNodeRef<T>, idx: usize) -> P::Prop {
        self.get_property_by_group::<P>(self.resolve_group_id(root_rel), idx)
    }
}
