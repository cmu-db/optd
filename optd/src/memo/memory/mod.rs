use super::{Memo, MemoError, MergeProducts, Representative, error::MemoResult};
use crate::cir::{
    Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId,
    LogicalProperties, PhysicalExpression, PhysicalExpressionId,
};
use futures::future::try_join_all;
use std::collections::HashMap;
use union_find::UnionFind;

mod materialize;
mod representative;
mod union_find;

/// An in-memory implementation of the memo table.
#[derive(Default)]
pub struct MemoryMemo {
    // Groups.
    /// Key is always a representative ID.
    group_info: HashMap<GroupId, GroupInfo>,
    /// To speed up lookups, we maintain a mapping from logical expression IDs to group IDs.
    /// Since this is an index, both key and value are representative IDs.
    logical_id_to_group_index: HashMap<LogicalExpressionId, GroupId>,

    // Goals.
    /// Key is always a representative ID.
    id_to_goal: HashMap<GoalId, Goal>,
    /// Each unique goal is mapped to a unique id, that may not be representative.
    /// This is used to speed up lookups, however it also means that the entries
    /// are never cleaned up. We leave this problem for later.
    goal_to_id: HashMap<Goal, GoalId>,

    // Expressions.
    /// Key is always a representative ID.
    id_to_logical_expr: HashMap<LogicalExpressionId, LogicalExpression>,
    /// Each unique expression is mapped to a unique id, that may not be representative.
    /// This is used to speed up lookups, however it also means that the entries
    /// are never cleaned up. We leave this problem for later.
    logical_expr_to_id: HashMap<LogicalExpression, LogicalExpressionId>,

    /// Key is always a representative ID.
    id_to_physical_expr: HashMap<PhysicalExpressionId, PhysicalExpression>,
    /// Each unique expression is mapped to a unique id, that may not be representative.
    /// This is used to speed up lookups, however it also means that the entries
    /// are never cleaned up. We leave this problem for later.
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
            .group_info
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?
            .logical_properties
            .clone())
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoResult<Vec<LogicalExpressionId>> {
        // Extract the vector of logical expression IDs from the group.
        let group_id = self.find_repr_group_id(group_id).await?;
        let expr_ids = self
            .group_info
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?
            .expressions
            .clone();

        // Update the vector with the representative IDs.
        let futures = expr_ids
            .into_iter()
            .map(|expr_id| async move { self.find_repr_logical_expr_id(expr_id).await });

        Ok(try_join_all(futures).await?)
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<Option<GroupId>> {
        let repr_logical_expr_id = self.find_repr_logical_expr_id(logical_expr_id).await?;
        Ok(self
            .logical_id_to_group_index
            .get(&repr_logical_expr_id)
            .copied())
    }

    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        props: &LogicalProperties,
    ) -> MemoResult<GroupId> {
        let group_id = GroupId(self.next_shared_id());
        let group_info = GroupInfo {
            expressions: vec![logical_expr_id],
            logical_properties: props.clone(),
        };

        self.group_info.insert(group_id, group_info);
        Ok(group_id)
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
        _goal_id: GoalId,
    ) -> MemoResult<Option<(PhysicalExpressionId, Cost)>> {
        todo!()
    }

    async fn get_all_goal_members(&self, _goal_id: GoalId) -> MemoResult<Vec<GoalMemberId>> {
        todo!()
    }

    async fn add_goal_member(
        &mut self,
        _goal_id: GoalId,
        _member: GoalMemberId,
    ) -> MemoResult<bool> {
        todo!()
    }

    async fn update_physical_expr_cost(
        &mut self,
        _physical_expr_id: PhysicalExpressionId,
        _new_cost: Cost,
    ) -> MemoResult<bool> {
        todo!()
    }

    async fn get_physical_expr_cost(
        &self,
        _physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<Option<Cost>> {
        todo!()
    }
}
