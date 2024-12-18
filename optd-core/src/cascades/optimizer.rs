// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use tracing::trace;

use super::memo::{ArcMemoPlanNode, GroupInfo, Memo};
use super::NaiveMemo;
use crate::cascades::memo::Winner;
use crate::cascades::tasks2::{TaskContext, TaskDesc};
use crate::cost::CostModel;
use crate::logical_property::{LogicalPropertyBuilder, LogicalPropertyBuilderAny};
use crate::nodes::{
    ArcPlanNode, ArcPredNode, NodeType, PlanNodeMeta, PlanNodeMetaMap, PlanNodeOrGroup,
};
use crate::optimizer::Optimizer;
use crate::physical_property::PhysicalProperty;
use crate::rules::Rule;

pub type RuleId = usize;

#[derive(Default, Clone, Debug)]
pub struct OptimizerContext {
    /// Not apply logical rules any more
    pub logical_budget_used: bool,
    /// Not apply all rules any more; get a physical plan ASAP
    pub all_budget_used: bool,
    pub rules_applied: usize,
}

#[derive(Default, Clone, Debug)]
pub struct OptimizerProperties {
    pub panic_on_budget: bool,
    /// If the number of rules applied exceeds this number, we stop applying logical rules.
    pub partial_explore_iter: Option<usize>,
    /// Plan space can be expanded by this number of times before we stop applying logical rules.
    pub partial_explore_space: Option<usize>,
    /// Disable pruning during optimization.
    pub disable_pruning: bool,
}

#[derive(Debug, Default)]
pub struct CascadesStats {
    pub rule_match_count: HashMap<usize, usize>,
    pub rule_total_bindings: HashMap<usize, usize>,
    pub explore_group_count: usize,
    pub optimize_group_count: usize,
    pub optimize_expr_count: usize,
    pub apply_rule_count: usize,
    pub optimize_input_count: usize,
}

pub struct CascadesOptimizer<T: NodeType, M: Memo<T> = NaiveMemo<T>> {
    memo: M,
    explored_group: HashSet<GroupId>,
    explored_expr: HashSet<TaskDesc>,
    fired_rules: HashMap<ExprId, HashSet<RuleId>>,
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    pub stats: CascadesStats,
    disabled_rules: HashSet<usize>,
    cost: Arc<dyn CostModel<T, M>>,
    logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
    pub ctx: OptimizerContext,
    pub prop: OptimizerProperties,
}

/// `RelNode` only contains the representation of the plan nodes. Sometimes, we need more context,
/// i.e., group id and expr id, during the optimization phase. All these information are collected
/// in this struct.
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PredId(pub usize);

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

impl Display for PredId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "P{}", self.0)
    }
}

impl<T: NodeType> CascadesOptimizer<T, NaiveMemo<T>> {
    pub fn new(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
    ) -> Self {
        Self::new_with_options(rules, cost, logical_property_builders, Default::default())
    }

    pub fn new_with_options(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        cost: Box<dyn CostModel<T, NaiveMemo<T>>>,
        logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
        prop: OptimizerProperties,
    ) -> Self {
        let memo = NaiveMemo::new(logical_property_builders.clone());
        Self {
            memo,
            explored_group: HashSet::new(),
            explored_expr: HashSet::new(),
            fired_rules: HashMap::new(),
            rules: rules.into(),
            cost: cost.into(),
            ctx: OptimizerContext::default(),
            logical_property_builders,
            prop,
            stats: CascadesStats::default(),
            disabled_rules: HashSet::new(),
        }
    }

    /// Clear the memo table and all optimizer states.
    pub fn step_clear(&mut self) {
        self.memo = NaiveMemo::new(self.logical_property_builders.clone());
        self.fired_rules.clear();
        self.explored_group.clear();
        self.explored_expr.clear();
    }

    /// Clear the winner so that the optimizer can continue to explore the group.
    pub fn step_clear_winner(&mut self) {
        self.memo.clear_winner();
        self.explored_group.clear();
        self.explored_expr.clear();
    }

    /// Clear the explored groups so that the optimizer can continue to apply the rules.
    pub fn step_next_stage(&mut self) {
        self.explored_group.clear();
        self.explored_expr.clear();
    }
}

impl<T: NodeType, M: Memo<T>> CascadesOptimizer<T, M> {
    pub fn panic_on_explore_limit(&mut self, enabled: bool) {
        self.prop.panic_on_budget = enabled;
    }

    pub fn disable_pruning(&mut self, enabled: bool) {
        self.prop.disable_pruning = enabled;
    }

    pub fn cost(&self) -> Arc<dyn CostModel<T, M>> {
        self.cost.clone()
    }

    pub fn rules(&self) -> Arc<[Arc<dyn Rule<T, Self>>]> {
        self.rules.clone()
    }

    pub fn disable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.insert(rule_id);
    }

    pub fn enable_rule(&mut self, rule_id: usize) {
        self.disabled_rules.remove(&rule_id);
    }

    pub fn disable_rule_by_name(&mut self, rule_name: &str) {
        let mut modified = false;
        for (id, rule) in self.rules.iter().enumerate() {
            if rule.name() == rule_name {
                self.disabled_rules.insert(id);
                modified = true;
            }
        }
        if !modified {
            panic!("rule {} not found", rule_name);
        }
    }

    pub fn enable_rule_by_name(&mut self, rule_name: &str) {
        let mut modified = false;
        for (id, rule) in self.rules.iter().enumerate() {
            if rule.name() == rule_name {
                self.disabled_rules.remove(&id);
                modified = true;
            }
        }
        if !modified {
            panic!("rule {} not found", rule_name);
        }
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
            for (id, property) in self.logical_property_builders.iter().enumerate() {
                println!(
                    "  {}={}",
                    property.property_name(),
                    group.properties[id].as_ref()
                )
            }
            let mut all_predicates = BTreeSet::new();
            for expr_id in self.memo.get_all_exprs_in_group(group_id) {
                let memo_node = self.memo.get_expr_memoed(expr_id);
                for pred in &memo_node.predicates {
                    all_predicates.insert(*pred);
                }
                println!("  expr_id={} | {}", expr_id, memo_node);
            }
            for pred in all_predicates {
                println!("  {}={}", pred, self.memo.get_pred(pred));
            }
        }
    }
    /// Optimize a `RelNode`.
    pub fn step_optimize_rel(&mut self, root_rel: ArcPlanNode<T>) -> Result<GroupId> {
        trace!(event = "step_optimize_rel", rel = %root_rel);
        let (group_id, _) = self.add_new_expr(root_rel);
        self.fire_optimize_tasks(group_id)?;
        Ok(group_id)
    }

    /// Gets the group binding.
    pub fn step_get_optimize_rel(
        &self,
        group_id: GroupId,
        meta: &mut Option<PlanNodeMetaMap>,
    ) -> Result<ArcPlanNode<T>> {
        let res = self
            .memo
            .get_best_group_binding(group_id, |node, group_id, info| {
                if let Some(meta) = meta {
                    let node = node.as_ref() as *const _ as usize;
                    let node_meta = PlanNodeMeta::new(
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

    pub fn fire_optimize_tasks(&mut self, group_id: GroupId) -> Result<()> {
        use pollster::FutureExt as _;
        trace!(event = "fire_optimize_tasks", root_group_id = %group_id);
        let mut task = TaskContext::new(self);
        // 32MB stack for the optimization process, TODO: reduce memory footprint
        stacker::maybe_grow(32 * 1024 * 1024, 32 * 1024 * 1024, || {
            let fut: Pin<Box<dyn Future<Output = ()>>> = Box::pin(task.fire_optimize(group_id));
            fut.block_on();
        });
        Ok(())
    }

    fn optimize_inner(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        let (group_id, _) = self.add_new_expr(root_rel);

        self.fire_optimize_tasks(group_id)?;
        self.memo.get_best_group_binding(group_id, |_, _, _| {})
    }

    pub fn resolve_group_id(&self, root_rel: PlanNodeOrGroup<T>) -> GroupId {
        root_rel.unwrap_group()
    }

    pub(super) fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        self.memo.get_all_exprs_in_group(group_id)
    }

    pub fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        self.memo.add_new_expr(rel_node)
    }

    pub fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        self.memo.add_expr_to_group(rel_node, group_id)
    }

    pub(super) fn get_group_winner(&self, group_id: GroupId) -> &Winner {
        self.memo.get_group_winner(group_id)
    }

    pub(super) fn update_group_winner(&mut self, group_id: GroupId, winner: Winner) {
        self.memo.update_group_info(group_id, GroupInfo { winner });
    }

    /// Get the properties of a Cascades group
    /// P is the type of the property you expect
    /// idx is the idx of the property you want. The order of properties is defined
    ///   by the property_builders parameter in CascadesOptimizer::new()
    pub fn get_property_by_group<P: LogicalPropertyBuilder<T>>(
        &self,
        group_id: GroupId,
        idx: usize,
    ) -> P::Prop {
        self.memo.get_group(group_id).properties[idx]
            .as_any()
            .downcast_ref::<P::Prop>()
            .unwrap()
            .clone()
    }

    pub(super) fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T> {
        self.memo.get_expr_memoed(expr_id)
    }

    pub fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        self.memo.get_pred(pred_id)
    }

    pub(super) fn is_group_explored(&self, group_id: GroupId) -> bool {
        self.explored_group.contains(&group_id)
    }

    pub(super) fn mark_group_explored(&mut self, group_id: GroupId) {
        self.explored_group.insert(group_id);
    }

    pub(super) fn has_task_started(&self, task_desc: &TaskDesc) -> bool {
        self.explored_expr.contains(task_desc)
    }

    pub(super) fn mark_task_start(&mut self, task_desc: &TaskDesc) {
        self.explored_expr.insert(task_desc.clone());
    }

    pub(super) fn mark_task_end(&mut self, task_desc: &TaskDesc) {
        self.explored_expr.remove(task_desc);
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

    pub fn dump_stats(&self) {
        println!("plan_space={}", self.memo.estimated_plan_space());
        for (id, rule) in self.rules.iter().enumerate() {
            println!(
                "{}: matched={}, bindings={}",
                rule.name(),
                self.stats
                    .rule_match_count
                    .get(&id)
                    .copied()
                    .unwrap_or_default(),
                self.stats
                    .rule_total_bindings
                    .get(&id)
                    .copied()
                    .unwrap_or_default()
            );
        }
        println!("explore_group_count={}", self.stats.explore_group_count);
        println!("optimize_group_count={}", self.stats.optimize_group_count);
        println!("optimize_expr_count={}", self.stats.optimize_expr_count);
        println!("apply_rule_count={}", self.stats.apply_rule_count);
        println!("optimize_input_count={}", self.stats.optimize_input_count);
    }
}

impl<T: NodeType, M: Memo<T>> Optimizer<T> for CascadesOptimizer<T, M> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        self.optimize_inner(root_rel)
    }

    fn optimize_with_required_props(
        &mut self,
        _: ArcPlanNode<T>,
        _: &[&dyn PhysicalProperty],
    ) -> Result<ArcPlanNode<T>> {
        unimplemented!()
    }

    fn get_logical_property<P: LogicalPropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop {
        self.get_property_by_group::<P>(self.resolve_group_id(root_rel), idx)
    }
}
