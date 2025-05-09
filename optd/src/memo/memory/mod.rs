use super::{Memo, MemoError, MergeProducts, Representative, error::MemoResult};
use crate::cir::{
    Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId,
    LogicalProperties, PhysicalExpression, PhysicalExpressionId,
};
use std::collections::HashMap;
use union_find::UnionFind;

mod materialize;
mod representative;
mod union_find;

/// An in-memory implementation of the memo table.
#[derive(Default)]
pub(crate) struct MemoryMemo {
    /// Groups.
    groups: HashMap<GroupId, GroupInfo>,

    // Goals.
    id_to_goal: HashMap<GoalId, Goal>,
    goal_to_id: HashMap<Goal, GoalId>,

    // Expressions.
    id_to_logical_expr: HashMap<LogicalExpressionId, LogicalExpression>,
    logical_expr_to_id: HashMap<LogicalExpression, LogicalExpressionId>,

    id_to_physical_expr: HashMap<PhysicalExpressionId, PhysicalExpression>,
    physical_expr_to_id: HashMap<PhysicalExpression, PhysicalExpressionId>,

    /// The shared next unique id to be used for goals, groups, logical expressions, and physical expressions.
    next_shared_id: i64,

    /// Representatives of groups, goals, and expression ids.
    repr_group_id: UnionFind<GroupId>,
    repr_goal_id: UnionFind<GoalId>,
    repr_logical_expr_id: UnionFind<LogicalExpressionId>,
    repr_physical_expr_id: UnionFind<PhysicalExpressionId>,
}

/// Information about a group:
/// - All logical expressions in this group
/// - Logical properties of this group
struct GroupInfo {
    expressions: Vec<LogicalExpressionId>,
    logical_properties: LogicalProperties,
}

impl MemoryMemo {
    fn next_shared_id(&mut self) -> i64 {
        let id = self.next_shared_id;
        self.next_shared_id += 1;
        id
    }
}

impl Memo for MemoryMemo {
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoResult<LogicalProperties> {
        let group_id = self.find_repr_group_id(group_id).await?;
        Ok(self
            .groups
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?
            .logical_properties
            .clone())
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoResult<Vec<LogicalExpressionId>> {
        todo!()
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<Option<GroupId>> {
        todo!()
    }

    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        props: &LogicalProperties,
    ) -> MemoResult<GroupId> {
        todo!()
    }

    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<MergeProducts> {
        todo!()
    }

    async fn get_best_optimized_physical_expr(
        &self,
        goal_id: GoalId,
    ) -> MemoResult<Option<(PhysicalExpressionId, Cost)>> {
        todo!()
    }

    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoResult<Vec<GoalMemberId>> {
        todo!()
    }

    async fn add_goal_member(&mut self, goal_id: GoalId, member: GoalMemberId) -> MemoResult<bool> {
        todo!()
    }

    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        new_cost: Cost,
    ) -> MemoResult<bool> {
        todo!()
    }

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<Option<Cost>> {
        todo!()
    }
}
