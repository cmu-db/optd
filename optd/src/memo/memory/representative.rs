//! The implementation of the [`Representative`] subtrait for the in-memory memo table.
//!
//! See the documentation for [`Representative`] for more information.

use super::{Infallible, MemoryMemo};
use crate::{cir::*, memo::Representative};

impl Representative for MemoryMemo {
    async fn find_repr_group_id(&self, group_id: GroupId) -> Result<GroupId, Infallible> {
        Ok(self.repr_group_id.find(&group_id))
    }

    async fn find_repr_goal_id(&self, goal_id: GoalId) -> Result<GoalId, Infallible> {
        Ok(self.repr_goal_id.find(&goal_id))
    }

    async fn find_repr_logical_expr_id(
        &self,
        expression_id: LogicalExpressionId,
    ) -> Result<LogicalExpressionId, Infallible> {
        Ok(self.repr_logical_expr_id.find(&expression_id))
    }

    async fn find_repr_physical_expr_id(
        &self,
        expression_id: PhysicalExpressionId,
    ) -> Result<PhysicalExpressionId, Infallible> {
        Ok(self.repr_physical_expr_id.find(&expression_id))
    }
}
