use super::MemoryMemo;
use crate::{
    cir::{
        Goal, GoalId, LogicalExpression, LogicalExpressionId, PhysicalExpression,
        PhysicalExpressionId,
    },
    memo::{Materialize, MemoError, Representative, error::MemoResult},
};

impl Materialize for MemoryMemo {
    async fn get_goal_id(&mut self, goal: &Goal) -> MemoResult<GoalId> {
        if let Some(&goal_id) = self.goal_to_id.get(goal) {
            return Ok(self.find_repr_goal_id(goal_id).await?);
        }

        let goal_id = GoalId(self.next_shared_id());
        self.id_to_goal.insert(goal_id, goal.clone());
        self.goal_to_id.insert(goal.clone(), goal_id);

        Ok(goal_id)
    }

    async fn materialize_goal(&self, goal_id: GoalId) -> MemoResult<Goal> {
        let repr_goal_id = self.find_repr_goal_id(goal_id).await?;
        self.id_to_goal
            .get(&repr_goal_id)
            .ok_or(MemoError::GoalNotFound(repr_goal_id))
            .cloned()
    }

    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoResult<LogicalExpressionId> {
        if let Some(&expr_id) = self.logical_expr_to_id.get(logical_expr) {
            return Ok(self.find_repr_logical_expr_id(expr_id).await?);
        }

        let expr_id = LogicalExpressionId(self.next_shared_id());
        self.id_to_logical_expr
            .insert(expr_id, logical_expr.clone());
        self.logical_expr_to_id
            .insert(logical_expr.clone(), expr_id);

        Ok(expr_id)
    }

    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<LogicalExpression> {
        let repr_expr_id = self.find_repr_logical_expr_id(logical_expr_id).await?;
        self.id_to_logical_expr
            .get(&repr_expr_id)
            .ok_or(MemoError::LogicalExprNotFound(repr_expr_id))
            .cloned()
    }

    async fn get_physical_expr_id(
        &mut self,
        physical_expr: &PhysicalExpression,
    ) -> MemoResult<PhysicalExpressionId> {
        if let Some(&expr_id) = self.physical_expr_to_id.get(physical_expr) {
            return Ok(self.find_repr_physical_expr_id(expr_id).await?);
        }

        let expr_id = PhysicalExpressionId(self.next_shared_id());
        self.id_to_physical_expr
            .insert(expr_id, physical_expr.clone());
        self.physical_expr_to_id
            .insert(physical_expr.clone(), expr_id);

        Ok(expr_id)
    }

    async fn materialize_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<PhysicalExpression> {
        let repr_expr_id = self.find_repr_physical_expr_id(physical_expr_id).await?;
        self.id_to_physical_expr
            .get(&repr_expr_id)
            .ok_or(MemoError::PhysicalExprNotFound(repr_expr_id))
            .cloned()
    }
}
