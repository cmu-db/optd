use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    sync::Arc,
};

use anyhow::Result;

use crate::{
    rel_node::{RelNodeRef, RelNodeTyp},
    rules::Rule,
};

use super::{memo::RelMemoNodeRef, tasks::OptimizeGroupTask, Memo, Task};

pub type RuleId = usize;

pub struct CascadesOptimizer<T: RelNodeTyp> {
    memo: Memo<T>,
    tasks: VecDeque<Box<dyn Task<T>>>,
    explored_group: HashSet<GroupId>,
    fired_rules: HashMap<ExprId, HashSet<RuleId>>,
    rules: Arc<[Arc<dyn Rule<T>>]>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct GroupId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct ExprId(pub usize);

impl Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for ExprId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: RelNodeTyp> CascadesOptimizer<T> {
    pub fn new_with_rules(rules: Vec<Arc<dyn Rule<T>>>) -> Self {
        let tasks = VecDeque::new();
        let memo = Memo::new();
        Self {
            memo,
            tasks,
            explored_group: HashSet::new(),
            fired_rules: HashMap::new(),
            rules: rules.into(),
        }
    }

    pub fn new() -> Self {
        Self::new_with_rules(vec![])
    }

    pub(super) fn rules(&self) -> Arc<[Arc<dyn Rule<T>>]> {
        self.rules.clone()
    }

    pub fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<Vec<RelNodeRef<T>>> {
        let (group_id, _) = self.memo.add_new_group_expr(root_rel, None);
        self.tasks
            .push_back(Box::new(OptimizeGroupTask::new(group_id)));
        // get the task from the stack
        while let Some(task) = self.tasks.pop_back() {
            let new_tasks = task.execute(self)?;
            self.tasks.extend(new_tasks);
        }
        for group_id in self.memo.get_all_group_ids() {
            println!("group_id={}", group_id);
            for expr_id in self.memo.get_all_exprs_in_group(group_id) {
                let memo_node = self.memo.get_expr_memoed(expr_id);
                println!("  expr_id={} | {}", expr_id, memo_node);
                let bindings = self.memo.get_all_expr_bindings(expr_id, false);
                for binding in bindings {
                    println!("    {}", binding);
                }
            }
        }
        Ok(self.memo.get_all_group_bindings(group_id, true))
    }

    pub(super) fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.memo.get_all_exprs_in_group(group_id)
    }

    pub(super) fn add_group_expr(
        &mut self,
        expr: RelNodeRef<T>,
        group_id: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        self.memo.add_new_group_expr(expr, group_id)
    }

    pub(super) fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        self.memo.get_group_id(expr_id)
    }

    pub(super) fn get_expr_memoed(&self, expr_id: ExprId) -> RelMemoNodeRef<T> {
        self.memo.get_expr_memoed(expr_id)
    }

    pub(super) fn get_all_expr_bindings(&self, expr_id: ExprId) -> Vec<RelNodeRef<T>> {
        self.memo.get_all_expr_bindings(expr_id, false)
    }

    pub(super) fn get_all_expr_physical_bindings(&self, expr_id: ExprId) -> Vec<RelNodeRef<T>> {
        self.memo.get_all_expr_bindings(expr_id, true)
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
