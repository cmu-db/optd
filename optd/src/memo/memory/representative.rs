use super::MemoryMemo;
use crate::{
    cir::{GoalId, GroupId, LogicalExpressionId, PhysicalExpressionId},
    memo::{Representative, error::MemoResult},
};

impl Representative for MemoryMemo {
    async fn find_repr_group_id(&self, group_id: GroupId) -> MemoResult<GroupId> {
        Ok(self.repr_group_id.find(&group_id))
    }

    async fn find_repr_goal_id(&self, goal_id: GoalId) -> MemoResult<GoalId> {
        Ok(self.repr_goal_id.find(&goal_id))
    }

    async fn find_repr_logical_expr_id(
        &self,
        expression_id: LogicalExpressionId,
    ) -> MemoResult<LogicalExpressionId> {
        Ok(self.repr_logical_expr_id.find(&expression_id))
    }

    async fn find_repr_physical_expr_id(
        &self,
        expression_id: PhysicalExpressionId,
    ) -> MemoResult<PhysicalExpressionId> {
        Ok(self.repr_physical_expr_id.find(&expression_id))
    }
}
