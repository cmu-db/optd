use super::Optimizer;
use crate::{
    cir::{Child, Goal, Operator, OptimizedExpression, PhysicalPlan},
    error::Error,
    memo::Memoize,
};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::sync::Arc;
use Child::*;

impl<M: Memoize> Optimizer<M> {
    /// Egest a physical plan from the memo table based on the best available physical expressions.
    ///
    /// This function reconstructs a complete physical plan by recursively retrieving
    /// the best optimized expressions for each goal referenced by the physical expression.
    ///
    /// # Arguments
    /// * `expr` - The optimized physical expression to egest
    ///
    /// # Returns
    /// * Ok(Some(PhysicalPlan)) if all child plans can be successfully materialized
    /// * Ok(None) if any child goal has no best expression
    /// * Err(Error) if a memo operation fails
    #[async_recursion]
    pub(super) async fn egest_best_plan(
        &self,
        expr: &OptimizedExpression,
    ) -> Result<Option<PhysicalPlan>, Error> {
        // Process all child goals to get their best physical plans
        let child_results = try_join_all(
            expr.0
                .children
                .iter()
                .map(|child| self.egest_child_plan(child)),
        )
        .await?;

        // If any child plan is None, return None
        let child_plans = match child_results.into_iter().collect() {
            Some(plans) => plans,
            None => return Ok(None),
        };

        // Construct the physical plan with the materialized children
        Ok(Some(PhysicalPlan(Operator {
            tag: expr.0.tag.clone(),
            data: expr.0.data.clone(),
            children: child_plans,
        })))
    }

    /// Helper function to egest a single child plan.
    ///
    /// This function handles both singleton and variable-length children,
    /// recursively retrieving the best physical plan for each goal.
    ///
    /// # Arguments
    /// * `child` - The child structure containing goals
    ///
    /// # Returns
    /// * Ok(Some(Child<Arc<PhysicalPlan>>)) if the goal can be successfully materialized
    /// * Ok(None) if the goal has no best expression
    /// * Err(Error) if a memo operation fails
    async fn egest_child_plan(
        &self,
        child: &Child<Goal>,
    ) -> Result<Option<Child<Arc<PhysicalPlan>>>, Error> {
        match child {
            Singleton(goal) => {
                let goal = self.goal_repr.find(goal);

                // Get the best optimized expression for this goal
                let best_expr = match self.memo.get_best_optimized_physical_expr(&goal).await? {
                    Some(expr) => expr,
                    None => return Ok(None),
                };

                // Recursively egest the plan for this expression
                let plan = match self.egest_best_plan(&best_expr).await? {
                    Some(plan) => plan,
                    None => return Ok(None),
                };

                Ok(Some(Singleton(plan.into())))
            }
            VarLength(goals) => {
                // For each goal, get its best physical plan
                let plan_futures = goals.iter().map(|goal| async move {
                    let goal = self.goal_repr.find(goal);

                    // Get the best optimized expression for this goal
                    let best_expr = match self.memo.get_best_optimized_physical_expr(&goal).await? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    };

                    // Recursively egest the plan for this expression
                    let plan = match self.egest_best_plan(&best_expr).await? {
                        Some(plan) => plan,
                        None => return Ok(None),
                    };

                    Ok(Some(plan.into()))
                });

                // Collect all plans
                let all_plans = try_join_all(plan_futures).await?;

                // If any plan is None, return None
                let plans = match all_plans.into_iter().collect() {
                    Some(plans) => plans,
                    None => return Ok(None),
                };

                Ok(Some(VarLength(plans)))
            }
        }
    }
}
