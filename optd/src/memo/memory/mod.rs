use super::{Memo, MemoError, MergeProducts, Representative, error::MemoResult};
use crate::{
    cir::{
        Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId,
        LogicalProperties, PhysicalExpression, PhysicalExpressionId,
    },
    memo::{Materialize, MergeGroupProduct},
};
use std::{
    collections::{HashMap, HashSet},
    vec,
};
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
    /// This function handles the cascading nature of group merges in a query optimizer memo:
    /// 1. When groups are merged, we create a new representative group
    /// 2. Logical expressions that reference merged groups need to be remapped
    /// 3. After remapping, previously distinct expressions may become equivalent
    /// 4. These newly equivalent expressions trigger additional group merges
    ///
    /// The implementation uses an iterative approach to process all merges.
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

                // Otherwise, we create new group with a fresh ID.
                let new_group_id = GroupId(self.next_shared_id());

                let group_info_1 = self
                    .group_info
                    .get(&group_id_1)
                    .ok_or(MemoError::GroupNotFound(group_id_1))?;
                let group_info_2 = self
                    .group_info
                    .get(&group_id_2)
                    .ok_or(MemoError::GroupNotFound(group_id_2))?;

                // Verify logical properties match (should be the case for equivalent groups),
                // unless the user defined unconsistent rules.
                debug_assert!(group_info_1.logical_properties == group_info_2.logical_properties);

                // Combine all expressions from both groups.
                let expr_ids_1 = self.get_all_logical_exprs(group_id_1).await?;
                let expr_ids_2 = self.get_all_logical_exprs(group_id_2).await?;
                let mut all_expr_ids = HashSet::with_capacity(expr_ids_1.len() + expr_ids_2.len());
                all_expr_ids.extend(expr_ids_1.iter().copied());
                all_expr_ids.extend(expr_ids_2.iter().copied());

                // Create and save new group.
                let new_group_info = GroupInfo {
                    expressions: all_expr_ids,
                    logical_properties: group_info_1.logical_properties.clone(),
                };
                self.group_info.insert(new_group_id, new_group_info);

                // Update all metadata structures to point to the new group.
                self.repr_group_id.merge(&group_id_1, &new_group_id);
                self.repr_group_id.merge(&group_id_2, &new_group_id);

                self.group_info.remove(&group_id_1);
                self.group_info.remove(&group_id_2);

                self.group_info[&new_group_id]
                    .expressions
                    .iter()
                    .for_each(|&expr_id| {
                        self.logical_id_to_group_index.insert(expr_id, new_group_id);
                    });

                // Merge the lists of expressions that reference these groups.
                let expr_set_1: HashSet<_> = self
                    .group_referencing_exprs_index
                    .remove(&group_id_1)
                    .unwrap_or_default()
                    .iter()
                    .map(|id| self.repr_logical_expr_id.find(id))
                    .collect();
                let expr_set_2: HashSet<_> = self
                    .group_referencing_exprs_index
                    .remove(&group_id_2)
                    .unwrap_or_default()
                    .iter()
                    .map(|id| self.repr_logical_expr_id.find(id))
                    .collect();
                let mut referenced_exprs = expr_set_1;
                referenced_exprs.extend(expr_set_2);
                self.group_referencing_exprs_index
                    .insert(new_group_id, referenced_exprs.clone());

                // Record this merge operation.
                merge_results.group_merges.push(MergeGroupProduct {
                    new_repr_group_id: new_group_id,
                    merged_groups: vec![group_id_1, group_id_2],
                });

                // Process cascading effects on expressions that referenced the merged groups.
                for reference_id in referenced_exprs {
                    // Remap the expression to use updated group references.
                    let prev_expr = self.materialize_logical_expr(reference_id).await?;
                    let new_expr = self.remap_logical_expr(&prev_expr).await?;
                    let new_id = self.get_logical_expr_id(&new_expr).await?;

                    if new_id != reference_id {
                        // If remapping created a different expression, update the union-find, and clear
                        // all old references to the old expression ID (except in group->expr index).
                        self.id_to_logical_expr.remove(&reference_id);
                        self.logical_expr_to_id.remove(&prev_expr);

                        let reference_group_opt =
                            self.find_logical_expr_group(reference_id).await?;
                        let new_group_opt = self.find_logical_expr_group(new_id).await?;

                        if let Some(reference_group) = reference_group_opt {
                            let group_info = self
                                .group_info
                                .get_mut(&reference_group)
                                .ok_or(MemoError::GroupNotFound(reference_group))?;

                            group_info.expressions.remove(&reference_id);
                            group_info.expressions.insert(new_id);

                            self.logical_id_to_group_index.remove(&reference_id);
                            if new_group_opt.is_none() {
                                self.logical_id_to_group_index
                                    .insert(new_id, reference_group);
                            }
                        }

                        self.repr_logical_expr_id.merge(&reference_id, &new_id);

                        match (new_group_opt, reference_group_opt) {
                            // Two different groups now contain equivalent expressions - merge them.
                            (Some(new_group), Some(reference_group))
                                if new_group != reference_group =>
                            {
                                pending_merges.push((new_group, reference_group));
                            }

                            _ => {} // Other cases don't require action.
                        }
                    }
                }
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
