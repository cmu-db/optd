use std::sync::Arc;

use futures::{
    stream::{self, Stream},
    StreamExt,
};

use anyhow::Result;
use async_recursion::async_recursion;
use tokio::task::JoinSet;

use super::memo::Memoize;

struct Driver<M: Memoize> {
    memo: M,
    rule_engine: Arc<Engine<M>>,
}

impl<'a, M: Memoize> Driver<M> {
    pub fn new(memo: M) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            memo,
            rule_engine: Arc::new(Intepreter {
                ctx: this.upgrade().unwrap(),
            }),
        })
    }

    /// The main entry point for the optimizer.
    /// If the cost is infinite, it will return None.
    /// If the cost is finite, it will return the best physical expression.
    pub async fn optimize(
        self: Arc<Self>,
        logical_plan: LogicalPlan,
    ) -> Result<Option<StoredPhysicalExpression>> {
        let group_id = ingest_full_logical_plan(&self.memo, &logical_plan).await?;
        let required_physical_props = PhysicalProperties { data: vec![] };
        self.optimize_goal(group_id, required_physical_props).await
    }

    /// This function is used to optimize a goal. It will be called by the entry point `optimize`.
    /// It will also be called by the interpreter when it needs to optimize a children goals.
    #[async_recursion]
    pub async fn optimize_goal(
        self: Arc<Self>,
        group_id: RelationalGroupId,
        required_physical_props: PhysicalProperties,
    ) -> Result<Option<StoredPhysicalExpression>> {
        let goal = self
            .memo
            .create_or_get_goal(group_id, required_physical_props)
            .await?;
        if self.memo.get_group_exploration_status(group_id).await? == ExplorationStatus::Unexplored
        {
            self.memo
                .update_group_exploration_status(goal.group_id, ExplorationStatus::Exploring)
                .await?;
            let ctx = self.clone();
            tokio::spawn(async move { ctx.explore_relation_group(group_id).await }).await?;
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
        logical_expr: Arc<LogicalExpression>,
        goal: Arc<Goal>,
    ) -> Result<(Cost, Option<StoredPhysicalExpression>)> {
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
                            .await?;
                        return Ok((Cost::INFINITY, None));
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
    ) -> Result<Vec<StoredLogicalExpression>> {
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
        logical_expr: Arc<LogicalExpression>,
        group_id: RelationalGroupId,
    ) -> Result<Vec<StoredLogicalExpression>> {
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
        logical_expr: Arc<LogicalExpression>,
        rule_id: TransformationRuleId,
        group_id: RelationalGroupId,
    ) -> Result<Vec<StoredLogicalExpression>> {
        let partial_logical_input = PartialLogicalPlan::from_expr(&logical_expr);

        let partial_logical_outputs = self
            .rule_engine
            .match_and_apply_transformation_rule(rule_id, partial_logical_input)
            .await;

        let mut partial_logical_outputs = Box::pin(partial_logical_outputs);

        let mut logical_exprs = Vec::new();
        while let Some(partial_logical_output) = partial_logical_outputs.next().await {
            let (logical_expr, logical_expr_id) =
                ingest_partial_logical_plan(&self.memo, &partial_logical_output).await?;
            logical_exprs.push((logical_expr, logical_expr_id));
        }

        Ok(logical_exprs)
    }

    pub async fn match_and_apply_implementation_rule(
        self: &Arc<Self>,
        logical_expr: Arc<LogicalExpression>,
        rule_id: ImplementationRuleId,
        goal: Arc<Goal>,
    ) -> Result<(Cost, Option<StoredPhysicalExpression>)> {
        let partial_logical_input = PartialLogicalPlan::from_expr(&logical_expr);

        let physical_outputs = self
            .rule_engine
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
            let (cost, top_phyiscal_expression) =
                ingest_partial_physical_plan(&self.memo, &physical_output).await?;
            if cost < best_cost {
                best_cost = cost;
                best_physical_output = Some(top_phyiscal_expression);
            }
        }

        Ok((best_cost, best_physical_output))
    }
}

struct Intepreter<M: Memoize> {
    ctx: Arc<Driver<M>>,
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
    ) -> impl StreamExt<Item = PartialPhysicalPlan> {
        let physical_plan = todo!();
        stream::once(async { physical_plan })
    }
}
