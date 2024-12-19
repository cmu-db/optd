use std::sync::Arc;

use itertools::Itertools;
use tracing::trace;

use super::memo::MemoPlanNode;
use super::rule_match::match_and_pick_expr;
use super::{optimizer::RuleId, CascadesOptimizer, ExprId, GroupId, Memo};
use crate::cascades::optimizer::OptimizerTrace;
use crate::cascades::{
    memo::{Winner, WinnerInfo},
    RelNodeContext,
};
use crate::cost::{Cost, Statistics};
use crate::nodes::ArcPredNode;
use crate::{nodes::NodeType, rules::RuleMatcher};

struct SearchContext {
    group_id: GroupId,
    upper_bound: Option<f64>,
}

pub struct TaskContext<'a, T: NodeType, M: Memo<T>> {
    optimizer: &'a mut CascadesOptimizer<T, M>,
    /// The stage of the process
    stage: usize,
    /// Number of tasks fired, used to determine the explore budget
    steps: usize,
    /// Counter of trace produced, used in the traces
    trace_steps: usize,
}

/// Ensures we don't run into cycles / dead loops.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TaskDesc {
    OptimizeExpr(ExprId, GroupId),
    OptimizeInput(ExprId, GroupId),
}

impl<'a, T: NodeType, M: Memo<T>> TaskContext<'a, T, M> {
    pub fn new(optimizer: &'a mut CascadesOptimizer<T, M>, stage: usize) -> Self {
        Self {
            stage,
            optimizer,
            steps: 0,
            trace_steps: 0,
        }
    }

    pub async fn fire_optimize(&mut self, group_id: GroupId) {
        tracing::debug!(event = "fire_optimize", group_id = %group_id);
        self.optimize_group(SearchContext {
            group_id,
            upper_bound: None,
        })
        .await;
    }

    async fn optimize_group(&mut self, ctx: SearchContext) {
        Box::pin(self.optimize_group_inner(ctx)).await;
    }

    async fn optimize_expr(&mut self, ctx: SearchContext, expr_id: ExprId, exploring: bool) {
        Box::pin(self.optimize_expr_inner(ctx, expr_id, exploring)).await;
    }

    async fn explore_group(&mut self, ctx: SearchContext) {
        Box::pin(self.explore_group_inner(ctx)).await;
    }

    async fn apply_rule(
        &mut self,
        ctx: SearchContext,
        rule_id: RuleId,
        expr_id: ExprId,
        exploring: bool,
    ) {
        Box::pin(self.apply_rule_inner(ctx, rule_id, expr_id, exploring)).await;
    }

    async fn optimize_input(&mut self, ctx: SearchContext, expr_id: ExprId) {
        Box::pin(self.optimize_input_inner(ctx, expr_id)).await;
    }

    async fn optimize_group_inner(&mut self, ctx: SearchContext) {
        self.steps += 1;
        self.optimizer.stats.optimize_group_count += 1;
        self.on_task_start();
        let SearchContext { group_id, .. } = ctx;
        trace!(event = "task_begin", task = "optimize_group", group_id = %group_id);

        if self.optimizer.is_group_explored(group_id) {
            trace!(
                event = "task_finish",
                task = "optimize_group",
                group_id = %group_id,
                outcome = "already explored, skipping",
            );
            return;
        }
        self.optimizer.mark_group_explored(group_id);

        // The Columbia optimizer will stop if we have a full winner, but given that we implement
        // 2-stage optimization, we will continue to optimize the group even if we have a full winner.

        let exprs = self.optimizer.get_all_exprs_in_group(group_id);
        // First, optimize all physical expressions
        for &expr_id in &exprs {
            let expr = self.optimizer.get_expr_memoed(expr_id);
            if !expr.typ.is_logical() {
                self.optimize_input(
                    SearchContext {
                        group_id,
                        upper_bound: ctx.upper_bound,
                    },
                    expr_id,
                )
                .await;
            }
        }
        // Then, optimize all logical expressions
        for &expr_id in &exprs {
            let typ = self.optimizer.get_expr_memoed(expr_id).typ.clone();
            if typ.is_logical() {
                self.optimize_expr(
                    SearchContext {
                        group_id,
                        upper_bound: ctx.upper_bound,
                    },
                    expr_id,
                    false,
                )
                .await
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %group_id);
    }

    async fn optimize_expr_inner(&mut self, ctx: SearchContext, expr_id: ExprId, exploring: bool) {
        self.steps += 1;
        self.optimizer.stats.optimize_expr_count += 1;
        self.on_task_start();
        let SearchContext { group_id, .. } = ctx;
        let desc = TaskDesc::OptimizeExpr(expr_id, group_id);
        if self.optimizer.has_task_started(&desc) {
            trace!(event = "task_skip", task = "optimize_expr", expr_id = %expr_id);
            return;
        }
        self.optimizer.mark_task_start(&desc);

        fn top_matches<T: NodeType>(matcher: &RuleMatcher<T>, match_typ: T) -> bool {
            match matcher {
                RuleMatcher::MatchNode { typ, .. } => typ == &match_typ,
                RuleMatcher::MatchDiscriminant {
                    typ_discriminant, ..
                } => std::mem::discriminant(&match_typ) == *typ_discriminant,
                _ => panic!("IR should have root node of match"),
            }
        }
        let expr = self.optimizer.get_expr_memoed(expr_id);
        assert!(expr.typ.is_logical());
        trace!(event = "task_begin", task = "optimize_expr", expr_id = %expr_id, expr = %expr);
        for (rule_id, rule) in self.optimizer.rules().iter().enumerate() {
            if self.optimizer.is_rule_fired(expr_id, rule_id) {
                continue;
            }
            // Skip impl rules when exploring
            if exploring && rule.is_impl_rule() {
                continue;
            }
            // Skip transformation rules when budget is used
            if (self.optimizer.ctx.logical_budget_used || self.optimizer.ctx.all_budget_used)
                && !rule.is_impl_rule()
            {
                continue;
            }
            if self.optimizer.ctx.all_budget_used
                && self.optimizer.get_group_winner(group_id).has_full_winner()
            {
                break;
            }
            if top_matches(rule.matcher(), expr.typ.clone()) {
                for &input_group_id in &expr.children {
                    self.explore_group(SearchContext {
                        group_id: input_group_id,
                        upper_bound: ctx.upper_bound,
                    })
                    .await;
                }
                self.apply_rule(
                    SearchContext {
                        group_id,
                        upper_bound: ctx.upper_bound,
                    },
                    rule_id,
                    expr_id,
                    exploring,
                )
                .await;
            }
        }
        self.optimizer.mark_task_end(&desc);
        trace!(event = "task_end", task = "optimize_expr", expr_id = %expr_id, expr = %expr);
    }

    async fn explore_group_inner(&mut self, ctx: SearchContext) {
        self.steps += 1;
        self.optimizer.stats.explore_group_count += 1;
        self.on_task_start();
        let SearchContext { group_id, .. } = ctx;
        trace!(event = "task_begin", task = "explore_group", group_id = %group_id);
        let exprs = self.optimizer.get_all_exprs_in_group(group_id);
        for expr in exprs {
            let typ = self.optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                self.optimize_expr(
                    SearchContext {
                        group_id,
                        upper_bound: ctx.upper_bound,
                    },
                    expr,
                    true,
                )
                .await;
            }
        }
        trace!(
            event = "task_finish",
            task = "explore_group",
            group_id = %group_id,
            outcome = "expanded group"
        );
    }

    async fn apply_rule_inner(
        &mut self,
        ctx: SearchContext,
        rule_id: RuleId,
        expr_id: ExprId,
        exploring: bool,
    ) {
        self.steps += 1;
        self.optimizer.stats.apply_rule_count += 1;
        self.on_task_start();
        let SearchContext { group_id, .. } = ctx;
        trace!(event = "task_begin", task = "apply_rule", expr_id = %expr_id, exploring = %exploring);
        if self.optimizer.is_rule_fired(expr_id, rule_id) {
            trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, exploring = %exploring, outcome = "rule already fired");
            return;
        }

        if self.optimizer.is_rule_disabled(rule_id) {
            trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, exploring = %exploring, outcome = "rule disabled");
            return;
        }

        self.optimizer.mark_rule_fired(expr_id, rule_id);

        let rule = self.optimizer.rules()[rule_id].clone();

        let binding_exprs = match_and_pick_expr(rule.matcher(), expr_id, self.optimizer);
        const BINDING_EXPR_WARNING_THRESHOLD: usize = 200;
        if binding_exprs.len() >= BINDING_EXPR_WARNING_THRESHOLD {
            tracing::warn!(
                event = "rule_application",
                task = "apply_rule",
                expr_id = %expr_id,
                rule_id = %rule_id,
                outcome = "too_many_bindings",
                num_bindings = %binding_exprs.len()
            );
        }
        if !binding_exprs.is_empty() {
            *self
                .optimizer
                .stats
                .rule_match_count
                .entry(rule_id)
                .or_default() += 1;
        }
        for binding in binding_exprs {
            *self
                .optimizer
                .stats
                .rule_total_bindings
                .entry(rule_id)
                .or_default() += 1;
            if !self.optimizer.ctx.logical_budget_used {
                let plan_space = self.optimizer.memo().estimated_plan_space();
                if let Some(partial_explore_space) = self.optimizer.prop.partial_explore_space {
                    if plan_space > partial_explore_space {
                        tracing::warn!(
                            "plan space size budget used, not applying logical rules any more. current plan space: {}",
                            plan_space
                        );
                        self.optimizer.ctx.logical_budget_used = true;
                        if self.optimizer.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
            }
            if !self.optimizer.ctx.all_budget_used {
                let step = self.steps;
                if let Some(partial_explore_iter) = self.optimizer.prop.partial_explore_iter {
                    if step > partial_explore_iter {
                        tracing::warn!(
                            "iter budget used, not applying any rules any more. current iter: {}",
                            step
                        );
                        self.optimizer.ctx.all_budget_used = true;
                        if self.optimizer.prop.panic_on_budget {
                            panic!("plan space size budget used");
                        }
                    }
                }
            }

            if (self.optimizer.ctx.logical_budget_used || self.optimizer.ctx.all_budget_used)
                && !rule.is_impl_rule()
            {
                continue;
            }
            if self.optimizer.ctx.all_budget_used
                && self.optimizer.get_group_winner(group_id).has_full_winner()
            {
                break;
            }

            trace!(event = "before_apply_rule", task = "apply_rule", input_binding=%binding);
            let applied = rule.apply(self.optimizer, binding);
            for expr in applied {
                trace!(event = "after_apply_rule", task = "apply_rule", output_binding=%expr);
                // TODO: remove clone in the below line
                if let Some(produced_expr_id) =
                    self.optimizer.add_expr_to_group(expr.clone(), group_id)
                {
                    if self.optimizer.prop.enable_tracing {
                        self.trace_steps += 1;
                        self.optimizer
                            .stats
                            .trace
                            .entry(group_id)
                            .or_default()
                            .push(OptimizerTrace::ApplyRule {
                                stage: self.stage,
                                step: self.trace_steps,
                                group_id,
                                applied_expr_id: expr_id,
                                produced_expr_id,
                                rule_id,
                            });
                    }
                    let typ = expr.unwrap_typ();
                    if typ.is_logical() {
                        self.optimize_expr(
                            SearchContext {
                                group_id,
                                upper_bound: ctx.upper_bound,
                            },
                            produced_expr_id,
                            exploring,
                        )
                        .await;
                    } else {
                        self.optimize_input(
                            SearchContext {
                                group_id,
                                upper_bound: ctx.upper_bound,
                            },
                            produced_expr_id,
                        )
                        .await;
                    }
                    trace!(event = "apply_rule", expr_id = %expr_id, rule_id = %rule_id, new_expr_id = %expr_id);
                } else {
                    trace!(event = "apply_rule", expr_id = %expr_id, rule_id = %rule_id, "triggered group merge");
                }
            }
        }
        trace!(event = "task_end", task = "apply_rule", expr_id = %expr_id, rule_id = %rule_id);
    }

    fn update_winner_if_better(&mut self, group_id: GroupId, proposed_winner: WinnerInfo) {
        let mut update_cost = false;
        let current_winner = self.optimizer.get_group_winner(group_id);
        if let Some(winner) = current_winner.as_full_winner() {
            if winner.total_weighted_cost > proposed_winner.total_weighted_cost {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            tracing::trace!(
                event = "update_winner",
                task = "optimize_inputs",
                expr_id = ?proposed_winner.expr_id,
                total_weighted_cost = %proposed_winner.total_weighted_cost,
                operation_weighted_cost = %proposed_winner.operation_weighted_cost,
            );
            self.optimizer
                .update_group_winner(group_id, Winner::Full(proposed_winner));
        }
    }

    #[allow(clippy::type_complexity)]
    fn gather_statistics_and_costs(
        &mut self,
        group_id: GroupId,
        expr_id: ExprId,
        expr: &MemoPlanNode<T>,
        predicates: &[ArcPredNode<T>],
    ) -> (
        Vec<Option<Arc<Statistics>>>,
        Vec<Cost>,
        Cost,
        Cost,
        Vec<Option<ExprId>>,
    ) {
        let context = RelNodeContext {
            expr_id,
            group_id,
            children_group_ids: expr.children.clone(),
        };
        let mut input_stats = Vec::with_capacity(expr.children.len());
        let mut input_cost = Vec::with_capacity(expr.children.len());
        let mut children_winners = Vec::with_capacity(expr.children.len());
        let cost = self.optimizer.cost();
        #[allow(clippy::needless_range_loop)]
        for idx in 0..expr.children.len() {
            let winner = self
                .optimizer
                .get_group_winner(expr.children[idx])
                .as_full_winner();
            let stats = winner.map(|x| x.statistics.clone());
            input_stats.push(stats.clone());
            input_cost.push(
                winner
                    .map(|x| x.total_cost.clone())
                    .unwrap_or_else(|| cost.zero()),
            );
            children_winners.push(winner.map(|x| x.expr_id));
        }
        let input_stats_ref = input_stats
            .iter()
            .map(|x| x.as_ref().map(|y| y.as_ref()))
            .collect_vec();
        let operation_cost = cost.compute_operation_cost(
            &expr.typ,
            predicates,
            &input_stats_ref,
            context.clone(),
            self.optimizer,
        );
        let total_cost = cost.sum(&operation_cost, &input_cost);
        (
            input_stats,
            input_cost,
            total_cost,
            operation_cost,
            children_winners,
        )
    }

    async fn optimize_input_inner(&mut self, ctx: SearchContext, expr_id: ExprId) {
        self.steps += 1;
        self.optimizer.stats.optimize_input_count += 1;
        self.on_task_start();
        let SearchContext { group_id, .. } = ctx;
        let desc = TaskDesc::OptimizeInput(expr_id, group_id);
        if self.optimizer.has_task_started(&desc) {
            trace!(event = "task_skip", task = "optimize_input",  expr_id = %expr_id);
            return;
        }
        self.optimizer.mark_task_start(&desc);

        trace!(event = "task_begin", task = "optimize_inputs",  expr_id = %expr_id);

        // TODO: assert this plan node satisfies subgoal

        let expr = self.optimizer.get_expr_memoed(expr_id);
        let cost = self.optimizer.cost();

        let predicates = expr
            .predicates
            .iter()
            .map(|pred_id| self.optimizer.get_pred(*pred_id))
            .collect_vec();

        // The upper bound of the search is the minimum of cost of the current best plan AND the
        // upper bound of the context.
        let winner_upper_bound = self
            .optimizer
            .memo()
            .get_group_winner(group_id)
            .as_full_winner()
            .map(|winner| winner.total_weighted_cost);

        let upper_bound = match (ctx.upper_bound, winner_upper_bound) {
            (Some(ub), Some(wub)) => Some(ub.min(wub)),
            (Some(ub), None) => Some(ub),
            (None, Some(wub)) => Some(wub),
            (None, None) => None,
        };

        for (input_group_idx, _) in expr.children.iter().enumerate() {
            // Before optimizing each of the child, infer a current lower bound cost
            let (_, input_costs, total_cost, _, _) =
                self.gather_statistics_and_costs(group_id, expr_id, &expr, &predicates);

            let child_upper_bound = if !self.optimizer.prop.disable_pruning {
                let cost_so_far = cost.weighted_cost(&total_cost);
                let child_current_cost = input_costs[input_group_idx].clone();
                // TODO: also adds up lower-bound cost
                trace!(
                    event = "compute_cost",
                    task = "optimize_inputs",
                    expr_id = %expr_id,
                    weighted_cost_so_far = cost_so_far,
                    upper_bound = ?upper_bound,
                    current_processing = %input_group_idx,
                    total_child_groups = %expr.children.len());
                if let Some(upper_bound) = upper_bound {
                    if upper_bound < cost_so_far {
                        // allow strictly == because we want to replan one of the child
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "pruned");
                        self.optimizer.mark_task_end(&desc);
                        return;
                    }
                    Some(upper_bound - cost_so_far + cost.weighted_cost(&child_current_cost))
                } else {
                    None
                }
            } else {
                None
            };

            let child_group_id = expr.children[input_group_idx];
            // always optimize group even if there's a winner b/c we want to replan (versus if there's a full winner then exit)
            self.optimize_group(SearchContext {
                group_id: child_group_id,
                upper_bound: child_upper_bound,
            })
            .await;
            let child_group_winner = self.optimizer.get_group_winner(child_group_id);
            if !child_group_winner.has_full_winner() {
                if let Winner::Unknown = self.optimizer.get_group_winner(child_group_id) {
                    self.optimizer.mark_task_end(&desc);
                    trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "impossible");
                    return;
                }
            }
        }

        // Compute everything again
        let (input_stats, _, total_cost, operation_cost, children_winner) =
            self.gather_statistics_and_costs(group_id, expr_id, &expr, &predicates);
        let input_stats_ref = input_stats
            .iter()
            .map(|x| {
                x.as_ref()
                    .expect("stats should be available for full winners")
                    .as_ref()
            })
            .collect_vec();
        let statistics = Arc::new(cost.derive_statistics(
            &expr.typ,
            &predicates,
            &input_stats_ref,
            RelNodeContext {
                expr_id,
                group_id,
                children_group_ids: expr.children.clone(),
            },
            self.optimizer,
        ));
        let proposed_winner = WinnerInfo {
            expr_id,
            total_cost: total_cost.clone(),
            operation_cost: operation_cost.clone(),
            total_weighted_cost: cost.weighted_cost(&total_cost),
            operation_weighted_cost: cost.weighted_cost(&operation_cost),
            statistics,
        };
        if self.optimizer.prop.enable_tracing {
            self.trace_steps += 1;
            self.optimizer
                .stats
                .trace
                .entry(group_id)
                .or_default()
                .push(OptimizerTrace::DecideWinner {
                    stage: self.stage,
                    step: self.trace_steps,
                    group_id,
                    proposed_winner_info: proposed_winner.clone(),
                    children_winner: children_winner.into_iter().map(|x| x.unwrap()).collect(),
                });
        }
        self.update_winner_if_better(group_id, proposed_winner);
        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %expr_id, result = "resolved");
        self.optimizer.mark_task_end(&desc);
    }

    fn on_task_start(&self) {
        if (self.optimizer.ctx.all_budget_used || self.optimizer.ctx.logical_budget_used)
            && self.steps % 100000 == 0
        {
            println!("out of budget, dumping info");
            println!("step={}", self.steps);
            self.optimizer.dump_stats();
        }
    }
}
