#![allow(unused)]
use super::Memoize;
use super::*;

pub struct MockMemo;

impl Memoize for MockMemo {
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties> {
        todo!()
    }

    async fn set_logical_properties(
        &self,
        group_id: GroupId,
        props: LogicalProperties,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpressionId>> {
        todo!()
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<Option<GroupId>> {
        todo!()
    }

    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<GroupId> {
        todo!()
    }

    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoizeResult<MergeResult> {
        todo!()
    }

    async fn get_best_optimized_physical_expr(
        &self,
        goal_id: GoalId,
    ) -> MemoizeResult<Option<(PhysicalExpressionId, Cost)>> {
        todo!()
    }

    async fn update_best_optimized_physical_expr(
        &mut self,
        goal_id: GoalId,
        physical_expr_id: PhysicalExpressionId,
        cost: Cost,
    ) -> MemoizeResult<bool> {
        todo!()
    }

    async fn get_all_physical_exprs(
        &self,
        goal_id: GoalId,
    ) -> MemoizeResult<Vec<PhysicalExpressionId>> {
        todo!()
    }

    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoizeResult<Vec<GoalMemberId>> {
        todo!()
    }

    async fn add_goal_member(
        &mut self,
        goal_id: GoalId,
        member: GoalMemberId,
    ) -> MemoizeResult<bool> {
        todo!()
    }

    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        new_cost: Cost,
    ) -> MemoizeResult<bool> {
        todo!()
    }

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Option<Cost>> {
        todo!()
    }

    async fn get_transformation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn get_implementation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    async fn set_implementation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn get_cost_status(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    async fn set_cost_clean(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn add_transformation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
        group_id: GroupId,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn add_implementation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
        group_id: GroupId,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn add_cost_dependency(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        goal_id: GoalId,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn get_goal_id(&mut self, goal: &Goal) -> MemoizeResult<GoalId> {
        todo!()
    }

    async fn materialize_goal(&self, goal_id: GoalId) -> MemoizeResult<Goal> {
        todo!()
    }

    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<LogicalExpressionId> {
        todo!()
    }

    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpression> {
        todo!()
    }

    async fn get_physical_expr_id(
        &mut self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<PhysicalExpressionId> {
        todo!()
    }

    async fn materialize_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpression> {
        todo!()
    }

    async fn find_repr_group(&self, group_id: GroupId) -> MemoizeResult<GroupId> {
        todo!()
    }

    async fn find_repr_goal(&self, goal_id: GoalId) -> MemoizeResult<GoalId> {
        todo!()
    }

    async fn find_repr_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpressionId> {
        todo!()
    }

    async fn find_repr_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpressionId> {
        todo!()
    }
}
