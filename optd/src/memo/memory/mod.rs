use super::{Memo, MemoError, MergeProducts, Representative, error::MemoResult};
use crate::cir::{
    Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId,
    LogicalProperties, PhysicalExpression, PhysicalExpressionId,
};
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
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

    // Indexes: only deal with representative IDs, but speeds up most queries.
    /// To speed up expr->group lookup, we maintain a mapping from logical expression IDs to group IDs.
    logical_id_to_group_index: HashMap<LogicalExpressionId, GroupId>,
    /// To speed up recursive merges, we maintain a mapping from group IDs to all logical expression IDs
    /// that contain a reference to this group.
    group_referencing_exprs_index: HashMap<GroupId, HashSet<LogicalExpressionId>>,

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
#[derive(Clone)]
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
        self.logical_id_to_group_index
            .insert(logical_expr_id, group_id);
        Ok(group_id)
    }

    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<MergeProducts> {
        // Get both representative group informations.
        let group_id_1 = self.find_repr_group_id(group_id_1).await?;
        let group_id_2 = self.find_repr_group_id(group_id_2).await?;

        if group_id_1 == group_id_2 {
            return Ok(MergeProducts::default());
        }

        // Perform the merge by creating a new group.
        let new_group_id = GroupId(self.next_shared_id());

        let group_info_1 = self
            .group_info
            .get(&group_id_1)
            .ok_or(MemoError::GroupNotFound(group_id_1))?;
        let group_info_2 = self
            .group_info
            .get(&group_id_2)
            .ok_or(MemoError::GroupNotFound(group_id_2))?;

        // Logical properties should always be the same when merging groups, or something
        // is wrong in the optimizer or rule definitions.
        debug_assert!(group_info_1.logical_properties == group_info_2.logical_properties);

        // First, get all logical expression IDs in both groups (reprs and dedup-ed).
        let expr_ids_1 = self.get_all_logical_exprs(group_id_1).await?;
        let expr_ids_2 = self.get_all_logical_exprs(group_id_2).await?;

        let mut all_expr_ids = HashSet::with_capacity(expr_ids_1.len() + expr_ids_2.len());
        all_expr_ids.extend(expr_ids_1.iter().copied());
        all_expr_ids.extend(expr_ids_2.iter().copied());

        let all_expr_ids = all_expr_ids.into_iter().collect();

        // Second, create a new group with the merged logical expressions,
        // and the same logical properties.
        let new_group_info = GroupInfo {
            expressions: all_expr_ids,
            logical_properties: group_info_1.logical_properties.clone(),
        };
        self.group_info.insert(new_group_id, new_group_info);

        // Update union-find structures.
        self.repr_group_id.merge(&group_id_1, &new_group_id);
        self.repr_group_id.merge(&group_id_2, &new_group_id);

        // Remove the old groups from the group info.
        self.group_info.remove(&group_id_1);
        self.group_info.remove(&group_id_2);

        // Adapt the logical expression to group index (automatically deletes the old ones).
        self.group_info[&new_group_id]
            .expressions
            .iter()
            .for_each(|&expr_id| {
                self.logical_id_to_group_index.insert(expr_id, new_group_id);
            });

        // Update group_referencing_exprs_index by taking the old entries, combining them,
        // and inserting the result as a new entry.
        let expr_set_1 = self
            .group_referencing_exprs_index
            .remove(&group_id_1)
            .unwrap_or_default();
        let expr_set_2 = self
            .group_referencing_exprs_index
            .remove(&group_id_2)
            .unwrap_or_default();

        let mut merged_set = expr_set_1;
        merged_set.extend(expr_set_2);
        self.group_referencing_exprs_index
            .insert(new_group_id, merged_set);

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
