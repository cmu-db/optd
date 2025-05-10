use super::{Memo, MemoError, MergeProducts, Representative, error::MemoResult};
use crate::cir::{
    Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId,
    LogicalProperties, PhysicalExpressionId,
};
use std::{
    collections::{HashMap, HashSet},
    vec,
};
use union_find::UnionFind;

mod helpers;
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
    /// Each representative goal is mapped to its id, for faster lookups.
    goal_to_id: HashMap<Goal, GoalId>,

    // Expressions.
    /// Key is always a representative ID.
    id_to_logical_expr: HashMap<LogicalExpressionId, LogicalExpression>,
    /// Each representantive expression is mapped to its id, for faster lookups.
    logical_expr_to_id: HashMap<LogicalExpression, LogicalExpressionId>,

    // Indexes: only deal with representative IDs, but speeds up most queries.
    /// To speed up expr->group lookup, we maintain a mapping from logical expression IDs to group IDs.
    logical_id_to_group_index: HashMap<LogicalExpressionId, GroupId>,
    /// To speed up recursive merges, we maintain a mapping from group IDs to all logical expression IDs
    /// that contain a reference to this group. The value logical_expr_ids may *NOT* be a representative ID.
    group_referencing_exprs_index: HashMap<GroupId, HashSet<LogicalExpressionId>>,

    /// The shared next unique id to be used for goals, groups, logical expressions, and physical expressions.
    next_shared_id: i64,

    /// Representatives of groups, goals, and expression ids, so that we can process old IDs.
    repr_group_id: UnionFind<GroupId>,
    repr_goal_id: UnionFind<GoalId>,
    repr_logical_expr_id: UnionFind<LogicalExpressionId>,
    repr_physical_expr_id: UnionFind<PhysicalExpressionId>,
}

/// Information about a group:
/// - All logical expressions in this group (always representative IDs).
/// - Logical properties of this group.
#[derive(Clone, Debug)]
struct GroupInfo {
    expressions: HashSet<LogicalExpressionId>,
    logical_properties: LogicalProperties,
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
    ) -> MemoResult<HashSet<LogicalExpressionId>> {
        let group_id = self.find_repr_group_id(group_id).await?;
        Ok(self
            .group_info
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?
            .expressions
            .clone())
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
        debug_assert!(
            self.logical_id_to_group_index
                .get(&logical_expr_id)
                .is_none(),
            "Logical expression ID already exists in a group."
        );

        let group_id = GroupId(self.next_shared_id());
        let group_info = GroupInfo {
            expressions: HashSet::from([logical_expr_id]),
            logical_properties: props.clone(),
        };

        self.group_info.insert(group_id, group_info);
        self.logical_id_to_group_index
            .insert(logical_expr_id, group_id);
        Ok(group_id)
    }

    /// Merges equivalent groups in the memo structure.
    ///
    /// This function implements a critical operation in query optimization: merging equivalent
    /// expression groups within the memo structure. When two groups are found to be equivalent,
    /// this function creates a new representative group and handles all the cascading effects.
    ///
    /// # Algorithm
    ///
    /// Merging groups has complex cascading effects because expressions in the memo reference
    /// groups by their IDs:
    ///
    /// 1. When groups A and B are merged into a new group C, all expressions that reference
    ///    group A or B need to be updated to reference group C instead.
    ///
    /// 2. After this update, expressions that were previously distinct might become identical.
    ///    This happens when the only difference between expressions was that one referenced
    ///    group A and the other referenced group B.
    ///
    /// 3. When expressions become identical, their containing groups also need to be merged,
    ///    creating a cascading effect.
    ///
    /// To handle this complexity, we use an iterative approach:
    /// - We maintain a queue of group pairs that need to be merged.
    /// - For each pair, we create a new representative group.
    /// - We update all expressions that reference the merged groups.
    /// - If this creates new equivalences, we add the affected groups to the merge queue.
    /// - We continue until no more merges are needed.
    ///
    /// This approach ensures that all cascading effects are properly handled and the memo
    /// structure remains consistent after the merge.
    ///
    /// # Parameters
    /// * `group_id_1` - ID of the first group to merge.
    /// * `group_id_2` - ID of the second group to merge.
    ///
    /// # Returns
    /// Detailed results of all merges performed, including cascading merges.
    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<MergeProducts> {
        let mut merge_results = MergeProducts::default();
        let mut pending_merges = vec![(group_id_1, group_id_2)];

        while !pending_merges.is_empty() {
            // Take ownership of the current pending merges, to avoid
            // borrowing issues with the mutable reference in the loop.
            let current_merges = std::mem::take(&mut pending_merges);

            for (g1, g2) in current_merges {
                // Find current representatives - skip if already merged.
                let group_id_1 = self.find_repr_group_id(g1).await?;
                let group_id_2 = self.find_repr_group_id(g2).await?;
                if group_id_1 == group_id_2 {
                    continue;
                }

                // Perform the group merge, creating a new representative
                let (new_group_id, merge_product) =
                    self.merge_group_pair(group_id_1, group_id_2).await?;
                merge_results.group_merges.push(merge_product);

                // Process expressions that reference the merged groups,
                // which may trigger additional group merges.
                let new_pending_merges = self
                    .process_referencing_expressions(group_id_1, group_id_2, new_group_id)
                    .await?;

                pending_merges.extend(new_pending_merges);
            }
        }

        Ok(merge_results)
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::cir::{Child, OperatorData};

    pub async fn lookup_or_insert(
        memo: &mut impl Memo,
        data: i64,
        children: Vec<GroupId>,
    ) -> GroupId {
        let expr = LogicalExpression {
            tag: data.to_string(),
            data: vec![OperatorData::Int64(data)],
            children: children.iter().map(|g| Child::Singleton(*g)).collect(),
        };

        let eid = memo.get_logical_expr_id(&expr).await.unwrap();
        match memo.find_logical_expr_group(eid).await.unwrap() {
            None => memo
                .create_group(eid, &LogicalProperties(None))
                .await
                .unwrap(),
            Some(g) => g,
        }
    }

    pub async fn insert_into_group(
        memo: &mut impl Memo,
        data: i64,
        children: Vec<GroupId>,
        gid: GroupId,
    ) -> GroupId {
        let g = lookup_or_insert(memo, data, children).await;
        memo.merge_groups(g, gid).await.unwrap();
        g
    }

    pub async fn retrieve(memo: &impl Memo, gid: GroupId) -> Vec<i64> {
        let eids = memo.get_all_logical_exprs(gid).await.unwrap();

        // Collect and sort data in expressions.
        let mut data = vec![];
        for eid in eids {
            let expr = memo.materialize_logical_expr(eid).await.unwrap();
            if let OperatorData::Int64(v) = expr.data[0] {
                data.push(v);
            }
        }
        data.sort();
        data
    }

    #[tokio::test]
    async fn test_lookup_same() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        let g1 = lookup_or_insert(&mut memo, 0, vec![]).await;

        assert_eq!(g0, g1);
    }

    #[tokio::test]
    async fn test_lookup_different() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        let g1 = lookup_or_insert(&mut memo, 1, vec![]).await;

        assert_ne!(g0, g1);
    }

    #[tokio::test]
    async fn test_add_to_group() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        insert_into_group(&mut memo, 1, vec![], g0).await;

        assert_eq!(retrieve(&memo, g0).await, vec![0, 1]);
    }

    #[tokio::test]
    async fn test_merge() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        insert_into_group(&mut memo, 1, vec![], g0).await;
        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;
        insert_into_group(&mut memo, 3, vec![], g2).await;
        let g4 = insert_into_group(&mut memo, 0, vec![], g2).await;

        assert_eq!(retrieve(&memo, g4).await, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_recursive_merge() {
        let mut memo = MemoryMemo::default();

        // g0: 0(), 1()
        // g4: 4(g0), 5()
        // g2: 2(), 3(), 1()
        // g5: 4(g2), 6()

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        let g0 = insert_into_group(&mut memo, 1, vec![], g0).await;
        let g4 = lookup_or_insert(&mut memo, 4, vec![g0]).await;
        insert_into_group(&mut memo, 5, vec![], g4).await;

        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;
        let g2 = insert_into_group(&mut memo, 3, vec![], g2).await;
        let g5 = lookup_or_insert(&mut memo, 4, vec![g2]).await;
        insert_into_group(&mut memo, 6, vec![], g5).await;

        // Trigger recursive merge.
        let g6 = insert_into_group(&mut memo, 1, vec![], g2).await;

        assert_eq!(retrieve(&memo, g6).await, vec![0, 1, 2, 3]);

        // Get merged upper group.
        let g7 = lookup_or_insert(&mut memo, 4, vec![g6]).await;

        assert_eq!(retrieve(&memo, g7).await, vec![4, 5, 6]);
    }
}
