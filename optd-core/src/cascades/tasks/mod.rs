use std::sync::Arc;

use crate::{
    cost_model::Cost,
    plans::{
        logical::PartialLogicalPlan,
        physical::{PartialPhysicalPlan, PhysicalPlan},
    },
};
use futures::{
    stream::{self, Stream},
    StreamExt,
};

use super::{
    expressions::{LogicalExpression, PhysicalExpression},
    get_physical_plan_cost,
    goal::{Goal, GoalId, OptimizationStatus},
    groups::{ExplorationStatus, RelationalGroupId},
    ingest_partial_logical_plan,
    memo::Memoize,
    mock_optimize_relation_expr,
    properties::PhysicalProperties,
    rules::{ImplementationRuleId, RuleId, TransformationRuleId},
};
use anyhow::Result;
use async_recursion::async_recursion;
use tokio::task::JoinSet;

struct TaskContext<M: Memoize> {
    memo: M,
    interpreter: Arc<Intepreter<M>>,
}

impl<'a, M: Memoize> TaskContext<M> {
    pub fn new(memo: M) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            memo,
            interpreter: Arc::new(Intepreter {
                ctx: this.upgrade().unwrap(),
            }),
        })
    }

    #[async_recursion]
    pub async fn optimize_goal(
        self: Arc<Self>,
        goal: Arc<Goal>,
    ) -> Result<(Cost, Option<PhysicalPlan>)> {
        let group_id = goal.group_id;
        if self.memo.get_group_exploration_status(group_id).await? == ExplorationStatus::Unexplored
        {
            self.memo
                .update_group_exploration_status(goal.group_id, ExplorationStatus::Exploring)
                .await?;
            let ctx = self.clone();
            tokio::spawn(async move { ctx.explore_relation_group(group_id).await }).await??;
        }

        let logical_exprs = self
            .memo
            .get_all_logical_exprs_in_group(goal.group_id)
            .await?;

        let mut join_set = JoinSet::new();
        for (logical_expr_id, logical_expr) in logical_exprs {
            let ctx = self.clone();
            let g = goal.clone();
            join_set.spawn(async move { ctx.optimize_logical_expression(logical_expr, g).await });
        }

        let plan_results = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let mut best_cost = Cost::INFINITY;
        let mut best_plan = None;
        for result in plan_results {
            if result.0 < best_cost {
                best_cost = result.0;
                best_plan = result.1;
            }
        }

        Ok((best_cost, best_plan))
    }

    #[async_recursion]
    pub async fn optimize_logical_expression(
        self: Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        goal: Arc<Goal>,
    ) -> Result<(Cost, Option<PhysicalPlan>)> {
        let rules = self.memo.get_matching_rules(&logical_expr).await?;

        let mut join_set = JoinSet::new();
        for rule in rules {
            let ctx = self.clone();
            let expr = logical_expr.clone();
            let g = goal.clone();
            match rule {
                RuleId::ImplementationRule(rule_id) => {
                    join_set.spawn(async move {
                        ctx.match_and_apply_implementation_rule(expr, rule_id, g)
                            .await
                    });
                }
                RuleId::TransformationRule(rule_id) => {
                    join_set.spawn(async move {
                        ctx.match_and_apply_transformation_rule(expr, rule_id, g.group_id)
                            .await
                    });
                }
            }
        }

        let plan_results = join_set
            .join_all()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>();

        let mut best_cost = Cost::INFINITY;
        let mut best_plan = None;
        for result in plan_results {
            if result.0 < best_cost {
                best_cost = result.0;
                best_plan = result.1;
            }
        }

        Ok((best_cost, best_plan))
    }

    #[async_recursion]
    pub async fn explore_relation_group(
        self: Arc<Self>,
        group_id: RelationalGroupId,
    ) -> Result<()> {
        let logical_exprs = self.memo.get_all_logical_exprs_in_group(group_id).await?;

        let mut join_set = JoinSet::new();
        for (logical_expr_id, logical_expr) in logical_exprs {
            let ctx = self.clone();
            join_set
                .spawn(async move { ctx.explore_logical_expression(logical_expr, group_id).await });
        }

        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    #[async_recursion]
    pub async fn explore_logical_expression(
        self: Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        group_id: RelationalGroupId,
    ) -> Result<()> {
        let rules = self
            .memo
            .get_matching_transformation_rules(&logical_expr)
            .await?;

        let mut join_set = JoinSet::new();
        for rule in rules {
            // Check if rule is applied, then:
            let ctx = self.clone();
            let expr = logical_expr.clone();
            join_set.spawn(async move {
                ctx.match_and_apply_transformation_rule(expr, rule, group_id)
                    .await
            });
        }

        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    pub async fn match_and_apply_transformation_rule(
        self: Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        rule_id: TransformationRuleId,
        group_id: RelationalGroupId,
    ) -> Result<(Cost, Option<PhysicalPlan>)> {
        let partial_logical_input = PartialLogicalPlan::from_expr(&logical_expr);

        let partial_logical_outputs = self
            .interpreter
            .match_and_apply_transformation_rule(rule_id, partial_logical_input)
            .await;

        let mut partial_logical_outputs = Box::pin(partial_logical_outputs);

        while let Some(partial_logical_output) = partial_logical_outputs.next().await {
            let group_id = ingest_partial_logical_plan(&self.memo, &partial_logical_output).await?;
        }

        Ok((Cost::INFINITY, None))
    }

    pub async fn match_and_apply_implementation_rule(
        self: &Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        rule_id: ImplementationRuleId,
        goal: Arc<Goal>,
    ) -> Result<(Cost, Option<PhysicalPlan>)> {
        let partial_logical_input = PartialLogicalPlan::from_expr(&logical_expr);

        let physical_outputs = self
            .interpreter
            .match_and_apply_implementation_rule(
                rule_id,
                partial_logical_input,
                &goal.required_physical_properties,
            )
            .await;

        let mut physical_outputs = Box::pin(physical_outputs);

        let mut best_cost = Cost::INFINITY;
        let mut best_physical_output = None;

        while let Some(physical_output) = physical_outputs.next().await {
            // TODO(Sarvesh): if we just get a physical plan, how can we ingest it?
            // we don't know the groups of the children will have to run some matching function to make sure we are ingesting the correct groups.
            // The correct way to ingest a physical plan is to break it down into physical expressions and ingest them one by one.
            // I don't want the interpreter to return a physical plan, I want it to return a physical expression.
            // Then we can ingest the physical expression directly.
            let cost = get_physical_plan_cost(&physical_output).await?;
            let group_id = self
                .memo
                .add_physical_plan_top_node(&physical_output, cost, goal.representative_goal_id)
                .await?;
            if cost < best_cost {
                best_cost = cost;
                best_physical_output = Some(physical_output);
            }
        }

        Ok((best_cost, best_physical_output))
    }

    pub async fn ingest_physical_expression(
        self: &Arc<Self>,
        physical_expr: &PhysicalExpression,
        cost: Cost,
        goal_id: GoalId,
    ) -> Result<()> {
        self.memo
            .add_physical_expr_to_goal(physical_expr, cost, goal_id)
            .await?;
        Ok(())
    }
}

struct Intepreter<M: Memoize> {
    ctx: Arc<TaskContext<M>>,
}

impl<M: Memoize> Intepreter<M> {
    async fn match_and_apply_transformation_rule(
        &self,
        rule_id: TransformationRuleId,
        partial_logical_plan: PartialLogicalPlan,
    ) -> impl StreamExt<Item = PartialLogicalPlan> {
        let transformed_plan = partial_logical_plan; // Replace with actual transformation logic
        stream::once(async { transformed_plan })
    }

    async fn match_and_apply_implementation_rule(
        &self,
        rule_id: ImplementationRuleId,
        partial_logical_plan: PartialLogicalPlan,
        required_physical_properties: &PhysicalProperties,
    ) -> impl StreamExt<Item = PhysicalPlan> {
        let physical_plan = todo!();
        stream::once(async { physical_plan })
    }
}
