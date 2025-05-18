//! Helper functions related to the in-memory memo table implementation.

use super::{Infallible, MemoryMemo};
use crate::{
    cir::*,
    memo::{
        Materialize, Memo, MergeGroupProduct, MergeProducts, Representative, memory::GroupInfo,
    },
};
use hashbrown::{HashMap, HashSet};
use std::borrow::Cow;

impl MemoryMemo {
    pub(super) fn next_group_id(&mut self) -> GroupId {
        let id = self.next_shared_id;
        self.next_shared_id += 1;
        GroupId(id)
    }

    pub(super) fn next_goal_id(&mut self) -> GoalId {
        let id = self.next_shared_id;
        self.next_shared_id += 1;
        GoalId(id)
    }

    pub(super) fn next_logical_expression_id(&mut self) -> LogicalExpressionId {
        let id = self.next_shared_id;
        self.next_shared_id += 1;
        LogicalExpressionId(id)
    }

    pub(super) fn next_physical_expression_id(&mut self) -> PhysicalExpressionId {
        let id = self.next_shared_id;
        self.next_shared_id += 1;
        PhysicalExpressionId(id)
    }

    /// Takes the set of [`LogicalExpressionId`] that reference a group, mapped to their
    /// representatives.
    fn take_referencing_expr_set(&mut self, group_id: GroupId) -> HashSet<LogicalExpressionId> {
        self.group_referencing_exprs_index
            .remove(&group_id)
            .unwrap_or_default()
            .iter()
            .map(|id| self.repr_logical_expr_id.find(id))
            .collect()
    }

    /// Merges the two sets of logical expressions that reference the two groups into a single set
    /// of expressions under a new [`GroupId`].
    ///
    /// If a group does not exist, then the set of expressions referencing it is the empty set.
    fn merge_referencing_exprs(&mut self, group1: GroupId, group2: GroupId, new_group: GroupId) {
        // Remove the entries for the original two groups that we want to merge.
        let exprs1 = self.take_referencing_expr_set(group1);
        let exprs2 = self.take_referencing_expr_set(group2);
        let new_set = exprs1.union(&exprs2).copied().collect();

        // Update the index for the new group / set of logical expressions.
        self.group_referencing_exprs_index
            .insert(new_group, new_set);
    }
}

/// Helper functions for the in-memory memo table implementation.
///
/// We split this out into a separate trait in order to have access to the associated error type.
///
/// See the implementation itself for the documentation of each helper method.
pub trait MemoryMemoHelper: Memo {
    async fn find_repr_goal_member_id(&self, id: GoalMemberId) -> Result<GoalMemberId, Infallible>;

    async fn remap_logical_expr<'a>(
        &self,
        logical_expr: &'a LogicalExpression,
    ) -> Result<Cow<'a, LogicalExpression>, Infallible>;

    async fn remap_physical_expr<'a>(
        &self,
        physical_expr: &'a PhysicalExpression,
    ) -> Result<Cow<'a, PhysicalExpression>, Infallible>;

    async fn remap_goal<'a>(&self, goal: &'a Goal) -> Result<Cow<'a, Goal>, Infallible>;

    async fn merge_group_pair(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> Result<(GroupId, MergeGroupProduct), Infallible>;

    async fn handle_expression_change(
        &mut self,
        old_id: LogicalExpressionId,
        new_id: LogicalExpressionId,
        prev_expr: LogicalExpression,
    ) -> Result<Option<(GroupId, GroupId)>, Infallible>;

    async fn process_referencing_expressions(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
        new_group_id: GroupId,
    ) -> Result<Vec<(GroupId, GroupId)>, Infallible>;

    async fn consolidate_merge_results(
        &self,
        merge_operations: Vec<MergeGroupProduct>,
    ) -> Result<MergeProducts, Infallible>;
}

impl MemoryMemoHelper for MemoryMemo {
    /// Finds the representative ID for a given [`GoalMemberId`].
    ///
    /// This handles both variants of `GoalMemberId` by finding the appropriate representative
    /// based on whether it's a Goal or PhysicalExpr.
    async fn find_repr_goal_member_id(&self, id: GoalMemberId) -> Result<GoalMemberId, Infallible> {
        use GoalMemberId::*;

        match id {
            GoalId(goal_id) => {
                let repr_goal_id = self.find_repr_goal_id(goal_id).await?;
                Ok(GoalId(repr_goal_id))
            }
            PhysicalExpressionId(expr_id) => {
                let repr_expr_id = self.find_repr_physical_expr_id(expr_id).await?;
                Ok(PhysicalExpressionId(repr_expr_id))
            }
        }
    }

    /// Remaps the children of a logical expression such that they are all identified by their
    /// representative IDs.
    ///
    /// For example, if a logical expression has a child group with [`GroupId`] 3, but the
    /// representative of group 3 is [`GroupId`] 42, then the output expression will be the input
    /// logical expression with a child group of 42.
    ///
    /// If no remapping needs to occur, this returns the same [`LogicalExpression`] object via the
    /// [`Cow`]. Otherwise, this function will create a new owned [`LogicalExpression`].
    async fn remap_logical_expr<'a>(
        &self,
        logical_expr: &'a LogicalExpression,
    ) -> Result<Cow<'a, LogicalExpression>, Infallible> {
        use Child::*;

        let mut needs_remapping = false;
        let mut remapped_children = Vec::with_capacity(logical_expr.children.len());

        for child in &logical_expr.children {
            let remapped = match child {
                Singleton(group_id) => {
                    let repr = self.find_repr_group_id(*group_id).await?;
                    needs_remapping |= repr != *group_id;
                    Singleton(repr)
                }
                VarLength(group_ids) => {
                    let mut reprs = Vec::with_capacity(group_ids.len());
                    for group_id in group_ids {
                        let repr = self.find_repr_group_id(*group_id).await?;
                        needs_remapping |= repr != *group_id;
                        reprs.push(repr);
                    }
                    VarLength(reprs)
                }
            };
            remapped_children.push(remapped);
        }

        Ok(if needs_remapping {
            Cow::Owned(LogicalExpression {
                tag: logical_expr.tag.clone(),
                data: logical_expr.data.clone(),
                children: remapped_children,
            })
        } else {
            Cow::Borrowed(logical_expr)
        })
    }

    /// Remaps the children of a physical expression such that they are all identified by their
    /// representative IDs.
    ///
    /// For example, if a physical expression has a child goal with [`GoalMemberId`] 3, but the
    /// representative of goal 3 is [`GoalMemberId`] 42, then the output expression will be the input
    /// physical expression with a child goal of 42.
    ///
    /// If no remapping needs to occur, this returns the same [`PhysicalExpression`] object via the
    /// [`Cow`]. Otherwise, this function will create a new owned [`PhysicalExpression`].
    async fn remap_physical_expr<'a>(
        &self,
        physical_expr: &'a PhysicalExpression,
    ) -> Result<Cow<'a, PhysicalExpression>, Infallible> {
        use Child::*;

        let mut needs_remapping = false;
        let mut remapped_children = Vec::with_capacity(physical_expr.children.len());

        for child in &physical_expr.children {
            let remapped = match child {
                Singleton(goal_id) => {
                    let repr = self.find_repr_goal_member_id(*goal_id).await?;
                    needs_remapping |= repr != *goal_id;
                    Singleton(repr)
                }
                VarLength(goal_ids) => {
                    let mut reprs = Vec::with_capacity(goal_ids.len());
                    for goal_id in goal_ids {
                        let repr = self.find_repr_goal_member_id(*goal_id).await?;
                        needs_remapping |= repr != *goal_id;
                        reprs.push(repr);
                    }
                    VarLength(reprs)
                }
            };
            remapped_children.push(remapped);
        }

        Ok(if needs_remapping {
            Cow::Owned(PhysicalExpression {
                tag: physical_expr.tag.clone(),
                data: physical_expr.data.clone(),
                children: remapped_children,
            })
        } else {
            Cow::Borrowed(physical_expr)
        })
    }

    /// Remaps the [`GroupId`] component of a [`Goal`] to its representative [`GroupId`].
    ///
    /// For example, if the [`Goal`] has a [`GroupID`] of 3, but the representative of group 3 is
    /// [`GroupId`] 42, then this goal's inner ID is remapped to `Goal(GroupId(42), properties)`.
    ///
    /// If no remapping needs to occur, this returns the same [`Goal`] object via the [`Cow`].
    /// Otherwise, this function will create a new owned [`Goal`].
    async fn remap_goal<'a>(&self, goal: &'a Goal) -> Result<Cow<'a, Goal>, Infallible> {
        let Goal(group_id, physical_properties) = goal;
        let repr_group_id = self.find_repr_group_id(*group_id).await?;

        // Only create a new `Goal` if it needs to be remapped.
        Ok(if group_id == &repr_group_id {
            Cow::Borrowed(goal)
        } else {
            Cow::Owned(Goal(repr_group_id, physical_properties.clone()))
        })
    }

    /// Merges a pair of groups, creating a new representative group that combines their logical
    /// expressions.
    ///
    /// Returns the new representative group ID and a record of the merge operation.
    ///
    /// In the memo structure, when two groups are found to be equivalent, we need to create a new
    /// representative group that combines their expressions and properties. This function handles
    /// that core operation.
    ///
    /// When groups are merged, various data structures need to be updated. This function updates
    /// the [`GroupInfo`] entries index, the union-find data structures that track representatives,
    /// and the index mapping logical expression IDs to groups.
    ///
    /// # Panics
    ///
    /// Panics if either of the input [`GroupId`]s are not found in the memo table.
    async fn merge_group_pair(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> Result<(GroupId, MergeGroupProduct), Infallible> {
        // Create a new group with a fresh ID.
        let new_group_id = self.next_group_id();

        // Take out the `GroupInfo` for both groups.
        let group1_info = self
            .group_info
            .remove(&group_id_1)
            .unwrap_or_else(|| panic!("{:?} not found in memo table", group_id_1));
        let group2_info = self
            .group_info
            .remove(&group_id_2)
            .unwrap_or_else(|| panic!("{:?} not found in memo table", group_id_2));

        // Verify that the logical properties match. This should be the case for equivalent groups,
        // unless there is a bug in the rules.
        assert_eq!(
            group1_info.logical_properties, group2_info.logical_properties,
            "Tried to merge groups that do not have the same logical properties. \
                There may be a bug in the rules"
        );

        // Combine expressions from both groups.
        let all_exprs: HashSet<_> = group1_info
            .expressions
            .union(&group2_info.expressions)
            .copied()
            .collect();

        // Update the union-find structure.
        self.repr_group_id.merge(&group_id_1, &new_group_id);
        self.repr_group_id.merge(&group_id_2, &new_group_id);

        // Update logical expression to group index.
        for &expr_id in &all_exprs {
            self.logical_id_to_group_index.insert(expr_id, new_group_id);
        }

        // Create and save the new group.
        let new_group_info = GroupInfo {
            expressions: all_exprs,
            goals: HashMap::new(), // TODO(Alexis): this will be used to trigger the goal merge process.
            logical_properties: group1_info.logical_properties,
        };
        self.group_info.insert(new_group_id, new_group_info);

        // Create a merge product record.
        let merge_product = MergeGroupProduct {
            new_group_id,
            merged_groups: vec![group_id_1, group_id_2],
        };

        Ok((new_group_id, merge_product))
    }

    /// Handles changes to an expression after remapping.
    ///
    /// Returns an optional pair of groups which we need to merge if the old and new expressions are
    /// in different groups.
    ///
    /// # Parameters
    ///
    /// * `old_id` - ID of the expression before remapping.
    /// * `new_id` - ID of the expression after remapping.
    /// * `prev_expr` - The expression before remapping.
    ///
    /// When an expression is remapped due to groups it references being merged, various data
    /// structures need to be updated. This function modifies the indexes mapping logical expression
    /// IDs, the index mapping logical expression ID to group, the union-find data structures
    /// that track representatives, and the group info entries.
    ///
    /// # Panics
    ///
    /// Panics if the [`LogicalExpressionId`] `old_id` does not belong to a real group.
    async fn handle_expression_change(
        &mut self,
        old_id: LogicalExpressionId,
        new_id: LogicalExpressionId,
        prev_expr: LogicalExpression,
    ) -> Result<Option<(GroupId, GroupId)>, Infallible> {
        // Remove the old expression from the logical expression indexes.
        self.id_to_logical_expr.remove(&old_id);
        self.logical_expr_to_id.remove(&prev_expr);

        // Get the groups that contain the old and new expressions.
        let old_group_opt = self.find_logical_expr_group(old_id).await?;
        let new_group_opt = self.find_logical_expr_group(new_id).await?;

        // Update the [`GroupInfo`] if the old expression was in a group.
        if let Some(old_group) = old_group_opt {
            let group_info = self
                .group_info
                .get_mut(&old_group)
                .unwrap_or_else(|| panic!("{:?} not found in memo table", old_group));

            // Remove the old expression from the group.
            group_info.expressions.remove(&old_id);
            self.logical_id_to_group_index.remove(&old_id);

            // Only add the new expression to the old expression's group if it does not exist in any
            // group yet.
            if new_group_opt.is_none() {
                group_info.expressions.insert(new_id);
                self.logical_id_to_group_index.insert(new_id, old_group);
            }
        }

        // Update the representative ID for the expression.
        self.repr_logical_expr_id.merge(&old_id, &new_id);

        // If the new and old expressions are in different groups, they need to be merged.
        // FIXME: Refactor this to use let-chains once it is stabilized in 1.88 (June 26, 2025)
        // https://github.com/rust-lang/rust/pull/132833#issuecomment-2824515215
        match (new_group_opt, old_group_opt) {
            (Some(new_group), Some(old_group)) if new_group != old_group => {
                Ok(Some((new_group, old_group)))
            }
            _ => Ok(None),
        }
    }

    /// Processes expressions that reference the merged groups.
    ///
    /// Returns the group pairs that need to be merged due to new equivalences.
    ///
    /// When groups are merged, expressions that reference them need to be updated. This function
    /// handles that update process and identifies any new equivalences that result.
    ///
    /// # Parameters
    ///
    /// * `group_id_1` - ID of the first merged group.
    /// * `group_id_2` - ID of the second merged group.
    /// * `new_group_id` - ID of the new representative group.
    ///
    /// # Algorithm
    ///
    /// The process involves several steps:
    /// 1. Collect all expressions that reference the merged groups.
    /// 2. Update the referencing expression index for the new group.
    /// 3. For each expression that referenced the merged groups:
    ///    a. Remap it to reference the new representative group.
    ///    b. Check if the remapped expression is different from the original.
    ///    c. If different, handle the change and check for new equivalences.
    /// 4. Return any new group pairs that need to be merged.
    async fn process_referencing_expressions(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
        new_group_id: GroupId,
    ) -> Result<Vec<(GroupId, GroupId)>, Infallible> {
        // Merge the set of expressions that reference these two groups into one that references the
        // new group.
        self.merge_referencing_exprs(group_id_1, group_id_2, new_group_id);

        // We need to clone here because we are modifying our `self` state inside the loop.
        // TODO: This is an inefficiency. This referencing index shouldn't be modified in the loop
        // below, but the borrow checker has no way of knowing that.
        let referencing_expressions = self
            .group_referencing_exprs_index
            .get(&new_group_id)
            .expect("we just created this referencing entry")
            .clone();

        let mut new_pending_merges = Vec::new();

        // Process each expression that referenced the merged groups.
        for reference_id in referencing_expressions {
            // Remap the expression to use updated group references.
            let prev_expr = self.materialize_logical_expr(reference_id).await?;
            let new_expr = self.remap_logical_expr(&prev_expr).await?;
            let new_id = self.get_logical_expr_id(&new_expr).await?;

            // If remapping created a different expression, handle the changes.
            if new_id != reference_id {
                let merge_pair = self
                    .handle_expression_change(reference_id, new_id, prev_expr)
                    .await?;
                if let Some(pair) = merge_pair {
                    new_pending_merges.push(pair);
                }
            }
        }

        Ok(new_pending_merges)
    }

    /// Consolidates merge operations into a comprehensive result.
    ///
    /// This function takes a list of individual merge operations and consolidates them into a
    /// complete picture of which groups were merged into each final representative.
    ///
    /// It also handles cases where past representatives themselves are merged into newer ones.
    async fn consolidate_merge_results(
        &self,
        merge_operations: Vec<MergeGroupProduct>,
    ) -> Result<MergeProducts, Infallible> {
        // Collect operations into a map from representative to all merged groups.
        let mut consolidated_map: HashMap<GroupId, HashSet<GroupId>> = HashMap::new();

        for op in merge_operations {
            let current_repr = self.find_repr_group_id(op.new_group_id).await?;

            consolidated_map
                .entry(current_repr)
                .or_default()
                .extend(op.merged_groups.iter().copied());

            if op.new_group_id != current_repr {
                consolidated_map
                    .entry(current_repr)
                    .or_default()
                    .insert(op.new_group_id);
            }
        }

        // Build the final list of merge products from the consolidated map.
        let group_merges = consolidated_map
            .into_iter()
            .filter_map(|(repr, groups)| {
                (!groups.is_empty()).then(|| MergeGroupProduct {
                    new_group_id: repr,
                    merged_groups: groups.into_iter().collect(),
                })
            })
            .collect();

        Ok(MergeProducts {
            group_merges,
            goal_merges: vec![],
            expr_merges: vec![],
        })
    }
}
