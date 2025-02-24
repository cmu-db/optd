use std::sync::Arc;

use crate::plans::{logical::PartialLogicalPlan, physical::PartialPhysicalPlan};

use super::{
    expressions::{LogicalExpression, PhysicalExpression},
    goal::{self, Goal, GoalId, OptimizationStatus},
    groups::{ExplorationStatus, RelationalGroupId},
    ingest_partial_logical_plan,
    memo::Memoize,
    properties::PhysicalProperties,
    rules::{ImplementationRuleId, TransformationRuleId},
};
use anyhow::Result;
use async_recursion::async_recursion;
use tokio::task::JoinSet;

struct TaskContext<M: Memoize> {
    memo: M,
    intepreter: Intepreter,
}

impl<'a, M: Memoize> TaskContext<M> {
    pub fn new(memo: M, intepreter: Intepreter) -> Arc<Self> {
        Arc::new(Self { memo, intepreter })
    }

    #[async_recursion]
    pub async fn optimize_goal(self: Arc<Self>, goal: Arc<Goal>) -> Result<()> {
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

        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
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
    pub async fn optimize_logical_expression(
        self: Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        goal: Arc<Goal>,
    ) -> Result<()> {
        let mut join_set = JoinSet::new();
        let group_id = goal.group_id;
        for child_relation in logical_expr.children_relations() {
            let status = self
                .memo
                .get_group_exploration_status(child_relation)
                .await?;
            if status == ExplorationStatus::Unexplored {
                self.memo
                    .update_group_exploration_status(child_relation, ExplorationStatus::Exploring)
                    .await?;
                let ctx = self.clone();
                join_set.spawn(async move { ctx.explore_relation_group(child_relation).await });
            }
        }

        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let rules = self
            .memo
            .get_matching_implementation_rules(&logical_expr)
            .await?;

        let mut join_set = JoinSet::new();
        for rule in rules {
            let ctx = self.clone();
            let expr = logical_expr.clone();
            let g = goal.clone();
            join_set.spawn(async move {
                ctx.match_and_apply_implementation_rule(
                    expr,
                    goal.representative_goal_id,
                    rule,
                    goal.required_physical_properties,
                )
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

    #[async_recursion]
    pub async fn explore_logical_expression(
        self: Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        group_id: RelationalGroupId,
    ) -> Result<()> {
        let mut join_set = JoinSet::new();
        for child_relation in logical_expr.children_relations() {
            let status = self
                .memo
                .get_group_exploration_status(child_relation)
                .await?;
            if status == ExplorationStatus::Unexplored {
                self.memo
                    .update_group_exploration_status(group_id, ExplorationStatus::Exploring)
                    .await?;
                let ctx = self.clone();
                join_set.spawn(async move { ctx.explore_relation_group(child_relation).await });
            }
        }

        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let rules = self
            .memo
            .get_matching_transformation_rules(&logical_expr)
            .await?;

        let mut join_set = JoinSet::new();
        for rule in rules {
            let ctx = self.clone();
            let expr = logical_expr.clone();
            join_set.spawn(async move {
                ctx.match_and_apply_transformation_rule(expr, group_id, rule)
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

    // pub async fn optimize_physical_expr(
    //     self: &Arc<Self>,
    //     physical_expr: PhysicalExpression,
    //     goal: &Arc<Goal>,
    // ) -> Result<()> {
    //     Ok(())
    // }

    pub async fn match_and_apply_transformation_rule(
        self: Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        group_id: RelationalGroupId,
        rule_id: TransformationRuleId,
    ) -> Result<()> {
        let partial_logical_input = PartialLogicalPlan::from_expr(&logical_expr);

        let partial_logical_outputs = self
            .intepreter
            .eval_transformation_rule(rule_id, partial_logical_input)
            .await;

        let mut join_set = JoinSet::new();
        for partial_logical_output in partial_logical_outputs {
            // TODO: need to return logical expression with id.
            let logical_expr_output = logical_expr.clone();
            let group_id = ingest_partial_logical_plan(&self.memo, &partial_logical_output).await?;
            let ctx = self.clone();
            join_set.spawn(async move {
                ctx.explore_logical_expression(logical_expr_output, group_id)
                    .await
            });
        }
        Ok(())
    }

    pub async fn match_and_apply_implementation_rule(
        self: &Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        rule_id: ImplementationRuleId,
        goal: Arc<Goal>,
    ) -> Result<()> {
        Ok(())
    }
}

struct Intepreter;

impl Intepreter {
    async fn eval_transformation_rule(
        &self,
        rule_id: TransformationRuleId,
        partial_logical_plan: PartialLogicalPlan,
    ) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    async fn eval_implementation_rule(
        &self,
        rule_id: ImplementationRuleId,
        partial_logical_plan: PartialLogicalPlan,
        required_physical_properties: PhysicalProperties,
    ) -> Vec<PartialPhysicalPlan> {
        todo!()
    }
}
