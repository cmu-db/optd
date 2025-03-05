use async_recursion::async_recursion;
use futures::StreamExt;
use optd_dsl::analyzer::{
    context::Context,
    hir::{GroupId, HIR},
};
use std::{char::MAX, sync::Arc};
use tokio::task::JoinSet;

use super::{
    ingest::{ingest_logical_operator, ingest_logical_plan},
    memo::Memoize,
};
use crate::{
    engine::{expander::Expander, Engine},
    error::Error,
    ir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::{Cost, ExplorationStatus},
        plans::{LogicalPlan, PartialLogicalPlan},
        properties::PhysicalProperties,
        rules::{ImplementationRule, RuleBook, TransformationRule},
    },
};

#[derive(Debug)]
pub struct Driver<M: Memoize> {
    memo: M,
    rule_book: RuleBook,
    engine: Engine<Arc<Self>>,
}

impl<M: Memoize> Driver<M> {
    pub fn new(memo: M, hir: HIR) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            memo,
            rule_book: RuleBook::default(),
            engine: Engine::new(hir.context, this.upgrade().unwrap()),
        })
    }

    pub async fn optimize(
        self: Arc<Self>,
        logical_plan: LogicalPlan,
    ) -> Result<Option<PhysicalExpression>, Error> {
        let group_id = ingest_logical_plan(&self.memo, &self.engine, &logical_plan.into()).await?;
        self.optimize_goal(Goal(group_id, PhysicalProperties(None)))
            .await
    }

    #[async_recursion]
    pub async fn optimize_goal(
        self: Arc<Self>,
        goal: Goal,
    ) -> Result<Option<PhysicalExpression>, Error> {
        /*let goal = self
            .memo
            .create_or_get_goal(group_id, &required_physical_props)
            .await?;
        if self.memo.get_group_exploration_status(group_id).await? == ExplorationStatus::Unexplored
        {
            self.memo
                .update_group_exploration_status(group_id, ExplorationStatus::Exploring)
                .await?;
            let ctx = self.clone();
            // TODO(sarvesh): we should use the result of this exploration instead of calling the memo table
            ctx.explore_relation_group(group_id).await?;
        }

        // TODO(sarvesh): we probably should get all logical expressions from the representative group
        let logical_exprs = self.memo.get_all_logical_exprs_in_group(group_id).await?;

        let mut join_set = JoinSet::new();
        for (logical_expr_id, logical_expr) in logical_exprs {
            let ctx = self.clone();
            let g = goal.clone();
            let required_physical_props = required_physical_props.clone();
            join_set.spawn(async move {
                ctx.optimize_expression(logical_expr, g, group_id, required_physical_props.clone())
                    .await
            });
        }

        let plan_results = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let mut best_plan = None;
        for result in plan_results {
            if result.0 < best_cost {
                best_cost = result.0;
                best_plan = result.1;
            }
        }

        Ok(best_plan)*/
        todo!()
    }

    /// This function is used to optimize a logical expression.
    /// It will be called by the `optimize_goal` function.
    /// It will return the best physical expression for the logical expression.
    /// If the cost is infinite, it will return None.
    /// If the cost is finite, it will return the best physical expression.
    #[async_recursion]
    pub async fn optimize_expression(
        self: Arc<Self>,
        logical_expr: LogicalExpression,
        goal: Goal,
        group_id: GroupId,
        req_props: Arc<PhysicalProperties>,
    ) -> Result<(Cost, Option<PhysicalExpression>), Error> {
        /*let rules = self.rule_book.get_implementations().iter().cloned();

        let mut join_set = JoinSet::new();
        for rule in rules {
            let ctx = self.clone();
            let logical_expr = logical_expr.clone();
            let req_props = req_props.clone();
            join_set.spawn(async move {
                ctx.match_and_apply_implementation_rule(logical_expr, rule, req_props)
                    .await
            });
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

        Ok((best_cost, best_plan))*/

        todo!()
    }

    /*#[async_recursion]
    pub async fn explore_relation_group(self: Arc<Self>, group_id: GroupId) -> Result<()> {
        let logical_exprs = self.memo.get_all_logical_exprs_in_group(group_id).await?;

        let mut join_set = JoinSet::new();
        for (_, logical_expr) in logical_exprs {
            let ctx = self.clone();
            join_set.spawn(async move { ctx.explore_expression(logical_expr, group_id).await });
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

        Ok(())
    }

    pub async fn explore_expression(
        self: Arc<Self>,
        logical_expr: LogicalExpression,
        group_id: GroupId,
    ) -> Result<Vec<(LogicalExpression, LogicalExpressionId)>> {
        let rules = vec![];

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
        rule: TransformationRule,
        group_id: GroupId,
    ) -> Result<Vec<(LogicalExpression, LogicalExpressionId)>> {
        let partial_logical_input = logical_expr.into();

        let mut partial_logical_outputs = self
            .engine
            .clone()
            .match_and_apply_logical_rule(&rule.0, partial_logical_input)
            .await;

        let mut logical_exprs_with_id = Vec::new();
        while let Some(partial_logical_output) = partial_logical_outputs.next().await {
            match partial_logical_output {
                Ok(partial_plan) => {
                    let new_group_id = match partial_plan {
                        PartialLogicalPlan::PartialMaterialized { node } => {
                            let (stored_logical_expr, logical_expr_id, new_group_id) =
                                ingest_logical_operator(&self.memo, &node).await?;
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
        rule: ImplementationRule,
        required_physical_props: Arc<PhysicalProperties>,
    ) -> Result<(Cost, Option<(PhysicalExpression, PhysicalExpressionId)>)> {
        let partial_logical_input = logical_expr.into();

        let mut physical_outputs = self
            .engine
            .clone()
            .match_and_apply_implementation_rule(
                &rule.0,
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
    }*/
}
