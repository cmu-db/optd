//! The implementation of the [`Materialize`] subtrait for the in-memory memo table.
//!
//! See the documentation for [`Materialize`] for more information.

use super::{Infallible, MemoryMemo, helpers::MemoryMemoHelper};
use crate::{
    cir::*,
    memo::{Materialize, Representative},
};

impl Materialize for MemoryMemo {
    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> Result<LogicalExpressionId, Infallible> {
        use Child::*;

        // Check if the expression is already in the memo table (fast path).
        let remapped_expr = self.remap_logical_expr(logical_expr).await?;
        if let Some(&expr_id) = self.logical_expr_to_id.get(remapped_expr.as_ref()) {
            return Ok(expr_id);
        }

        // Otherwise, create a new entry in the memo table (slow path).
        let expr_id = self.next_logical_expression_id();

        // Update the logical expression to group index.
        remapped_expr
            .children
            .iter()
            .flat_map(|child| match child {
                Singleton(group_id) => vec![*group_id],
                VarLength(group_ids) => group_ids.clone(),
            })
            .for_each(|group_id| {
                self.group_referencing_exprs_index
                    .entry(group_id)
                    .or_default()
                    .insert(expr_id);
            });

        // Update the logical expression ID indexes.
        self.id_to_logical_expr
            .insert(expr_id, remapped_expr.clone().into_owned());
        self.logical_expr_to_id
            .insert(remapped_expr.into_owned(), expr_id);

        Ok(expr_id)
    }

    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> Result<LogicalExpression, Infallible> {
        let repr_expr_id = self.find_repr_logical_expr_id(logical_expr_id).await?;
        Ok(self
            .id_to_logical_expr
            .get(&repr_expr_id)
            .unwrap_or_else(|| panic!("{:?} not found in memo table", repr_expr_id))
            .clone())
    }

    async fn get_goal_id(&mut self, goal: &Goal) -> Result<GoalId, Infallible> {
        // Check if the goal is already in the memo table (fast path).
        let goal = self.remap_goal(goal).await?;
        if let Some(&goal_id) = self.goal_to_id.get(goal.as_ref()) {
            return Ok(goal_id);
        }

        // Otherwise, create a new entry in the memo table (slow path).
        let goal_id = self.next_goal_id();
        self.id_to_goal.insert(goal_id, goal.clone().into_owned());
        self.goal_to_id.insert(goal.into_owned(), goal_id);

        Ok(goal_id)
    }

    async fn materialize_goal(&self, goal_id: GoalId) -> Result<Goal, Infallible> {
        let repr_goal_id = self.find_repr_goal_id(goal_id).await?;
        Ok(self
            .id_to_goal
            .get(&repr_goal_id)
            .unwrap_or_else(|| panic!("{:?} not found in memo table", repr_goal_id))
            .clone())
    }

    async fn get_physical_expr_id(
        &mut self,
        _physical_expr: &PhysicalExpression,
    ) -> Result<PhysicalExpressionId, Infallible> {
        todo!()
    }

    async fn materialize_physical_expr(
        &self,
        _physical_expr_id: PhysicalExpressionId,
    ) -> Result<PhysicalExpression, Infallible> {
        todo!()
    }
}
