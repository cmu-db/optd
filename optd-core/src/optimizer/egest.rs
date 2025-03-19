use super::{memo::Memoize, Optimizer};
use crate::{
    cir::{
        expressions::OptimizedExpression,
        goal::Goal,
        operators::{Child, Operator},
        plans::PhysicalPlan,
    },
    error::Error,
};
use async_recursion::async_recursion;
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
        &mut self,
        expr: &OptimizedExpression,
    ) -> Result<Option<PhysicalPlan>, Error> {
        // Process all children goals recursively
        let mut child_plans = Vec::with_capacity(expr.0.children.len());
        for child in &expr.0.children {
            let child_plan = self.egest_child_plan(child).await?;
            match child_plan {
                Some(plan) => child_plans.push(plan),
                None => return Ok(None),
            }
        }

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
        &mut self,
        child: &Child<Goal>,
    ) -> Result<Option<Child<Arc<PhysicalPlan>>>, Error> {
        match child {
            Singleton(goal) => {
                // Get the best optimized expression for this goal
                let goal_id = self.memo.get_goal_id(&goal).await?;
                let best_expr = match self.memo.get_best_optimized_physical_expr(goal_id).await? {
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
                let mut result_plans = Vec::with_capacity(goals.len());
                for goal in goals {
                    let goal_id = self.memo.get_goal_id(goal).await?;

                    // Get the best optimized expression for this goal
                    let best_expr =
                        match self.memo.get_best_optimized_physical_expr(goal_id).await? {
                            Some(expr) => expr,
                            None => return Ok(None),
                        };

                    // Recursively egest the plan for this expression
                    let plan = match self.egest_best_plan(&best_expr).await? {
                        Some(plan) => plan,
                        None => return Ok(None),
                    };

                    result_plans.push(plan.into());
                }

                Ok(Some(VarLength(result_plans)))
            }
        }
    }
}
