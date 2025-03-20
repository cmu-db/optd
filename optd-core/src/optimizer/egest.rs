use super::{memo::Memoize, Optimizer};
use crate::{
    cir::{
        expressions::PhysicalExpressionId,
        goal::GoalId,
        operators::{Child, Operator},
        plans::PhysicalPlan,
    },
    error::Error,
};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::sync::Arc;
use Child::*;

impl<M: Memoize> Optimizer<M> {
    /// Recursively transforms a physical expression ID in the memo into a complete physical plan.
    ///
    /// This function retrieves the physical expression from the memo and recursively
    /// transforms any child goal IDs into their corresponding best physical plans.
    ///
    /// # Returns
    /// * `Ok(Some(PhysicalPlan))` if all child plans were successfully constructed from their IDs
    /// * `Ok(None)` if any goal ID lacks a best expression ID
    /// * `Err(Error)` if a memo operation fails
    #[async_recursion]
    pub(super) async fn egest_best_plan(
        &self,
        expression_id: PhysicalExpressionId,
    ) -> Result<Option<PhysicalPlan>, Error> {
        let expression = self.memo.materialize_physical_expr(expression_id).await?;

        // Recursively egest all children plans.
        let child_results = try_join_all(
            expression
                .children
                .iter()
                .map(|child| self.egest_child_plan(child)),
        )
        .await?;

        let child_plans = match child_results.into_iter().collect::<Option<Vec<_>>>() {
            Some(plans) => plans,
            None => return Ok(None),
        };

        // Base case: construct the physical plan with the materialized children.
        Ok(Some(PhysicalPlan(Operator {
            tag: expression.tag,
            data: expression.data,
            children: child_plans,
        })))
    }

    /// Transforms a child reference containing goal IDs into a child reference containing physical plans.
    ///
    /// Handles both singleton and variable-length collections of goal IDs.
    async fn egest_child_plan(
        &self,
        child: &Child<GoalId>,
    ) -> Result<Option<Child<Arc<PhysicalPlan>>>, Error> {
        match child {
            Singleton(goal_id) => {
                let (best_expr_id, _) =
                    match self.memo.get_best_optimized_physical_expr(*goal_id).await? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    };

                let plan = match self.egest_best_plan(best_expr_id).await? {
                    Some(plan) => plan,
                    None => return Ok(None),
                };

                Ok(Some(Singleton(plan.into())))
            }
            VarLength(goals) => {
                let futures = goals.iter().map(|goal_id| async move {
                    let (best_expr_id, _) =
                        match self.memo.get_best_optimized_physical_expr(*goal_id).await? {
                            Some(expr) => expr,
                            None => return Ok(None),
                        };

                    let plan = match self.egest_best_plan(best_expr_id).await? {
                        Some(plan) => plan,
                        None => return Ok(None),
                    };

                    Ok(Some(plan.into()))
                });

                let result_plans = match try_join_all(futures).await?.into_iter().collect() {
                    Some(plans) => plans,
                    None => return Ok(None),
                };

                Ok(Some(VarLength(result_plans)))
            }
        }
    }
}
