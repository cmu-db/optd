use anyhow::Result;
use async_recursion::async_recursion;
use futures::StreamExt;
use optd_dsl::analyzer::{context::Context, hir::GroupId};
use std::{char::MAX, sync::Arc};
use tokio::task::JoinSet;

use super::memo::Memoize;
use crate::{
    engine::{expander::Expander, Engine},
    ir::{
        cost::{Cost, MAX_COST},
        expressions::{
            LogicalExpression, LogicalExpressionId, PhysicalExpression, PhysicalExpressionId,
        },
        goal::PhysicalGoalId,
        groups::{ExplorationStatus, LogicalGroupId},
        plans::{LogicalPlan, PartialLogicalPlan},
        properties::{PhysicalProperties, PropertiesData},
        rules::{ImplementationRuleId, RuleId, TransformationRuleId},
    },
};

#[derive(Debug, Clone)]
pub struct Driver<M: Memoize> {
    pub memo: M,
    pub rule_engine: Engine<Arc<Self>>,
}

impl<M: Memoize> Driver<M> {
    pub fn new(memo: M) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            memo,
            rule_engine: Engine::new(Context::default(), this.upgrade().unwrap()),
        })
    }
    /// The main entry point for the optimizer.
    /// If the cost is infinite, it will return None.
    /// If the cost is finite, it will return the best physical expression.
    pub async fn optimize(
        self: Arc<Self>,
        logical_plan: LogicalPlan,
    ) -> Result<Option<(PhysicalExpression, PhysicalExpressionId)>> {
        let group_id = todo!(); // ingest_full_logical_plan(&self.memo, &logical_plan).await?;
        let required_physical_props = Arc::new(PhysicalProperties(None));
        self.optimize_goal(group_id, required_physical_props).await
    }

    /// This function is used to optimize a goal. It will be called by the entry point `optimize`.
    /// It will also be called by the interpreter when it needs to optimize a children goals.
    #[async_recursion]
    pub async fn optimize_goal(
        self: Arc<Self>,
        group_id: LogicalGroupId,
        required_physical_props: Arc<PhysicalProperties>,
    ) -> Result<Option<(PhysicalExpression, PhysicalExpressionId)>> {
        let goal = self
            .memo
            .create_or_get_goal(group_id, required_physical_props.clone())
            .await?;
        if self.memo.get_group_exploration_status(group_id).await? == ExplorationStatus::Unexplored
        {
            self.memo
                .update_group_exploration_status(group_id, ExplorationStatus::Exploring)
                .await?;
            let ctx = self.clone();
            // TODO(sarvesh): we should use the result of this exploration instead of calling the memo table
            let _ = tokio::spawn(async move { ctx.explore_relation_group(group_id).await }).await?;
        }

        // TODO(sarvesh): we probably should get all logical expressions from the representative group
        let logical_exprs = self.memo.get_all_logical_exprs_in_group(group_id).await?;

        let mut join_set = JoinSet::new();
        for (logical_expr_id, logical_expr) in logical_exprs {
            let ctx = self.clone();
            let g = goal.clone();
            let required_physical_props = required_physical_props.clone();
            join_set.spawn(async move {
                ctx.optimize_logical_expression(logical_expr, g, group_id, required_physical_props)
                    .await
            });
        }

        let plan_results = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let mut best_cost = MAX_COST;
        let mut best_plan = None;
        for result in plan_results {
            if result.0 < best_cost {
                best_cost = result.0;
                best_plan = result.1;
            }
        }

        Ok(best_plan)
    }

    /// This function is used to optimize a logical expression.
    /// It will be called by the `optimize_goal` function.
    /// It will return the best physical expression for the logical expression.
    /// If the cost is infinite, it will return None.
    /// If the cost is finite, it will return the best physical expression.
    #[async_recursion]
    pub async fn optimize_logical_expression(
        self: Arc<Self>,
        logical_expr: LogicalExpression,
        goal: PhysicalGoalId,
        group_id: LogicalGroupId,
        required_physical_props: Arc<PhysicalProperties>,
    ) -> Result<(Cost, Option<(PhysicalExpression, PhysicalExpressionId)>)> {
        let rules = self.memo.get_matching_rules(&logical_expr).await?;

        let mut join_set = JoinSet::new();
        for rule in rules {
            let ctx = self.clone();
            let g = goal.clone();
            // we clone here because we have to clone eventually in match_and_apply for the into conversion
            let logical_expr = logical_expr.clone();
            let props = required_physical_props.clone();
            match rule {
                RuleId::ImplementationRule(rule_id) => {
                    join_set.spawn(async move {
                        ctx.match_and_apply_implementation_rule(logical_expr, rule_id, &props)
                            .await
                    });
                }
                RuleId::TransformationRule(rule_id) => {
                    join_set.spawn(async move {
                        ctx.match_and_apply_transformation_rule(logical_expr, rule_id, group_id)
                            .await?;
                        return Ok((MAX_COST, None));
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

        let mut best_cost = MAX_COST;
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
        group_id: LogicalGroupId,
    ) -> Result<Vec<(LogicalExpression, LogicalExpressionId)>> {
        let logical_exprs = self.memo.get_all_logical_exprs_in_group(group_id).await?;

        let mut join_set = JoinSet::new();
        for (logical_expr_id, logical_expr) in logical_exprs {
            let ctx = self.clone();
            join_set
                .spawn(async move { ctx.explore_logical_expression(logical_expr, group_id).await });
        }

        let results = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let mut logical_exprs = Vec::new();
        for result in results {
            logical_exprs.extend(result);
        }

        Ok(logical_exprs)
    }

    #[async_recursion]
    pub async fn explore_logical_expression(
        self: Arc<Self>,
        logical_expr: LogicalExpression,
        group_id: LogicalGroupId,
    ) -> Result<Vec<(LogicalExpression, LogicalExpressionId)>> {
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

        let results = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let mut logical_exprs = Vec::new();
        for result in results {
            logical_exprs.extend(result);
        }

        Ok(logical_exprs)
    }

    pub async fn match_and_apply_transformation_rule(
        self: Arc<Self>,
        logical_expr: LogicalExpression,
        rule_id: TransformationRuleId,
        group_id: LogicalGroupId,
    ) -> Result<Vec<(LogicalExpression, LogicalExpressionId)>> {
        let partial_logical_input = logical_expr.into();

        let mut partial_logical_outputs = self
            .rule_engine
            .clone()
            .match_and_apply_logical_rule(&rule_id.0, partial_logical_input)
            .await;

        let mut logical_exprs_with_id = Vec::new();
        while let Some(partial_logical_output) = partial_logical_outputs.next().await {
            match partial_logical_output {
                Ok(partial_plan) => {
                    let new_group_id = match partial_plan {
                        PartialLogicalPlan::PartialMaterialized { node } => {
                            let (stored_logical_expr, logical_expr_id, new_group_id) =
                            todo!(); //ingest_partial_logical_plan(&self.memo, &node, Some(group_id))
                                   // .await?;
                            logical_exprs_with_id.push((stored_logical_expr, logical_expr_id));
                            new_group_id
                        }
                        PartialLogicalPlan::UnMaterialized(new_group_id) => new_group_id,
                    };
                    if new_group_id != group_id {
                        self.memo
                            .merge_relation_group(group_id, new_group_id)
                            .await?;
                    }
                }
                Err(e) => {
                    println!("DSL Error: {:?}", e);
                    todo!()
                }
            }
        }

        Ok(logical_exprs_with_id)
    }

    pub async fn match_and_apply_implementation_rule(
        self: &Arc<Self>,
        logical_expr: LogicalExpression,
        rule_id: ImplementationRuleId,
        required_physical_props: &PhysicalProperties,
    ) -> Result<(Cost, Option<(PhysicalExpression, PhysicalExpressionId)>)> {
        let partial_logical_input = logical_expr.into();

        let mut physical_outputs = self
            .rule_engine
            .clone()
            .match_and_apply_implementation_rule(
                &rule_id.0,
                partial_logical_input,
                required_physical_props,
            )
            .await;

        let mut best_cost = MAX_COST;
        let mut best_physical_output = None;

        while let Some(physical_output) = physical_outputs.next().await {
            match physical_output {
                Ok(partial_physical_output) => {
                    let (cost, top_phyiscal_expression) = todo!();
                        // ingest_partial_physical_plan(&self.memo, &partial_physical_output).await?;
                    if cost < best_cost {
                        best_cost = cost;
                        best_physical_output = Some(top_phyiscal_expression);
                    }
                }
                Err(e) => {
                    println!("DSL Error: {:?}", e);
                    todo!()
                }
            }
        }

        Ok((best_cost, best_physical_output))
    }
}
