//! Helper functions related to the in-memory memo table implementation.

use super::{Infallible, MemoryMemo};
use crate::{
    cir::*,
    memo::{
        Materialize, Memo, MergeGoalProduct, MergeGroupProduct, MergePhysicalExprProduct,
        Representative, memory::GroupInfo,
    },
};
use hashbrown::{HashMap, HashSet};
use std::{borrow::Cow, collections::VecDeque};

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
    fn find_repr_goal_member_id(&self, id: GoalMemberId) -> GoalMemberId;

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

    async fn process_referencing_logical_exprs(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
        new_group_id: GroupId,
    ) -> Result<Vec<(GroupId, GroupId)>, Infallible>;

    async fn consolidate_merge_group_products(
        &self,
        merge_operations: Vec<MergeGroupProduct>,
    ) -> Result<Vec<MergeGroupProduct>, Infallible>;

    async fn merge_dependent_goals(
        &mut self,
        group_merges: &[MergeGroupProduct],
    ) -> Result<Vec<MergeGoalProduct>, Infallible>;

    async fn consolidate_merge_physical_expr_products(
        &self,
        merge_operations: Vec<MergePhysicalExprProduct>,
    ) -> Result<Vec<MergePhysicalExprProduct>, Infallible>;

    async fn merge_dependent_physical_exprs(
        &mut self,
        goal_merges: &[MergeGoalProduct],
    ) -> Result<Vec<MergePhysicalExprProduct>, Infallible>;
}

impl MemoryMemoHelper for MemoryMemo {
    /// Finds the representative ID for a given [`GoalMemberId`].
    ///
    /// This handles both variants of `GoalMemberId` by finding the appropriate representative
    /// based on whether it's a Goal or PhysicalExpr.
    fn find_repr_goal_member_id(&self, id: GoalMemberId) -> GoalMemberId {
        use GoalMemberId::*;

        match id {
            GoalId(goal_id) => {
                let repr_goal_id = self.repr_goal_id.find(&goal_id);
                GoalId(repr_goal_id)
            }
            PhysicalExpressionId(expr_id) => {
                let repr_expr_id = self.repr_physical_expr_id.find(&expr_id);
                PhysicalExpressionId(repr_expr_id)
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

        let remapped_children = physical_expr
            .children
            .iter()
            .map(|child| match child {
                Singleton(goal_id) => {
                    let repr = self.find_repr_goal_member_id(*goal_id);
                    if repr != *goal_id {
                        needs_remapping = true;
                    }
                    Singleton(repr)
                }
                VarLength(goal_ids) => {
                    let reprs: Vec<_> = goal_ids
                        .iter()
                        .map(|id| {
                            let repr = self.find_repr_goal_member_id(*id);
                            if repr != *id {
                                needs_remapping = true;
                            }
                            repr
                        })
                        .collect();
                    VarLength(reprs)
                }
            })
            .collect();

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

        // Combine goals from both groups.
        let all_goals: HashMap<_, Vec<_>> = group1_info
            .goals
            .into_iter()
            .chain(group2_info.goals)
            .fold(HashMap::new(), |mut acc, (props, mut goals)| {
                acc.entry(props)
                    .and_modify(|existing_goals| existing_goals.append(&mut goals))
                    .or_insert(goals);
                acc
            });

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
            goals: all_goals,
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

    /// Processes logical expressions that reference the merged groups.
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
    async fn process_referencing_logical_exprs(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
        new_group_id: GroupId,
    ) -> Result<Vec<(GroupId, GroupId)>, Infallible> {
        // Merge the set of expressions that reference these two groups into one
        // that references the new group.
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

    /// Consolidates merge group operations into a comprehensive result.
    ///
    /// This function takes a list of individual group merge operations and consolidates
    /// them into a complete picture of which groups were merged into each final representative.
    ///
    /// It also handles cases where past representatives themselves are merged into newer ones.
    async fn consolidate_merge_group_products(
        &self,
        merge_operations: Vec<MergeGroupProduct>,
    ) -> Result<Vec<MergeGroupProduct>, Infallible> {
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

        Ok(group_merges)
    }

    /// Processes goal merges and returns the results.
    ///
    /// This function performs the merge operations for goals based on the merged groups and
    /// returns a list of `MergeGoalProduct` instances representing the merged goals.
    ///
    /// # Parameters
    ///
    /// * `group_merges` - A slice of `MergeGroupProduct` instances representing the merged groups.
    ///
    /// # Returns
    ///
    /// A vector of `MergeGoalProduct` instances representing the merged goals.
    async fn merge_dependent_goals(
        &mut self,
        group_merges: &[MergeGroupProduct],
    ) -> Result<Vec<MergeGoalProduct>, Infallible> {
        let mut goal_merges = Vec::new();

        for merge_product in group_merges {
            let new_group_id = merge_product.new_group_id;
            let group_info = self.group_info.get_mut(&new_group_id).unwrap();

            // Take related goals to the group to avoid borrowing issues.
            let related_goals = std::mem::take(&mut group_info.goals);
            let mut updated_goals = HashMap::new();

            for (physical_props, merged_goals) in related_goals {
                // Create a new representative goal for this physical property.
                let new_goal = Goal(new_group_id, physical_props.clone());
                let new_goal_id = self.get_goal_id(&new_goal).await?;

                // Process each goal being merged.
                for &old_goal_id in &merged_goals {
                    // Remove old goal info and extend new goal with its members.
                    let old_goal_info = self.goal_info.remove(&old_goal_id).unwrap();
                    let new_goal_info = self.goal_info.get_mut(&new_goal_id).unwrap();

                    new_goal_info.members.extend(old_goal_info.members);
                    self.goal_to_id.remove(&old_goal_info.goal);

                    // Update referencing expressions index.
                    let refs = self
                        .goal_member_referencing_exprs_index
                        .remove(&GoalMemberId::GoalId(old_goal_id))
                        .unwrap_or_default();

                    self.goal_member_referencing_exprs_index
                        .entry(GoalMemberId::GoalId(new_goal_id))
                        .or_default()
                        .extend(refs);

                    // Update the union-find structure for goal representatives.
                    self.repr_goal_id.merge(&old_goal_id, &new_goal_id);
                }

                // Update the goals mapping for this physical property.
                updated_goals.insert(physical_props, vec![new_goal_id]);

                // Record the merge operation.
                goal_merges.push(MergeGoalProduct {
                    new_goal_id,
                    merged_goals,
                });
            }

            // Update the group's goals with the consolidated mapping.
            self.group_info.get_mut(&new_group_id).unwrap().goals = updated_goals;
        }

        Ok(goal_merges)
    }

    /// Consolidates merge physical expression operations into a comprehensive result.
    ///
    /// This function takes a list of individual physical expression merge operations
    /// and consolidates them into a complete picture of which physical expressions
    /// were merged into each final representative.
    ///
    /// It also handles cases where past representatives themselves are merged into newer ones.
    async fn consolidate_merge_physical_expr_products(
        &self,
        merge_operations: Vec<MergePhysicalExprProduct>,
    ) -> Result<Vec<MergePhysicalExprProduct>, Infallible> {
        // Collect operations into a map from representative to all merged physical expressions.
        let mut consolidated_map: HashMap<PhysicalExpressionId, HashSet<PhysicalExpressionId>> =
            HashMap::new();

        for op in merge_operations {
            let current_repr = self.repr_physical_expr_id.find(&op.new_physical_expr_id);

            consolidated_map
                .entry(current_repr)
                .or_default()
                .extend(op.merged_physical_exprs.iter().copied());

            if op.new_physical_expr_id != current_repr {
                consolidated_map
                    .entry(current_repr)
                    .or_default()
                    .insert(op.new_physical_expr_id);
            }
        }

        // Build the final list of merge products from the consolidated map.
        let physical_expr_merges = consolidated_map
            .into_iter()
            .filter_map(|(repr, exprs)| {
                (!exprs.is_empty()).then(|| MergePhysicalExprProduct {
                    new_physical_expr_id: repr,
                    merged_physical_exprs: exprs.into_iter().collect(),
                })
            })
            .collect();

        Ok(physical_expr_merges)
    }

    /// Processes physical expression merges and returns the results.
    ///
    /// This function performs the merge operations for physical expressions based on
    /// the merged goals and returns a list of `MergePhysicalExprProduct` instances
    /// representing the merged physical expressions.
    ///
    /// # Parameters
    ///
    /// * `goal_merges` - A slice of `MergeGoalProduct` instances representing the merged goals.
    ///
    /// # Returns
    ///
    /// A vector of `MergePhysicalExprProduct` instances representing the merged physical expressions.
    async fn merge_dependent_physical_exprs(
        &mut self,
        goal_merges: &[MergeGoalProduct],
    ) -> Result<Vec<MergePhysicalExprProduct>, Infallible> {
        let mut physical_expr_merges = Vec::new();
        let mut pending_dependencies = VecDeque::new();

        // Initialize pending dependencies with the input goal merges.
        for goal_merge in goal_merges {
            pending_dependencies.push_back(GoalMemberId::GoalId(goal_merge.new_goal_id));
        }

        // Process dependencies in a loop to handle cascading merges.
        while let Some(current_dependency) = pending_dependencies.pop_front() {
            let referencing_exprs = self
                .goal_member_referencing_exprs_index
                .get(&current_dependency)
                .cloned()
                .unwrap_or_default();

            // Process each expression that references this dependency.
            for reference_id in referencing_exprs {
                // Remap the expression to use updated member references.
                let prev_expr = self.materialize_physical_expr(reference_id).await?;
                let new_expr = self.remap_physical_expr(&prev_expr).await?;
                let new_id = self.get_physical_expr_id(&new_expr).await?;

                if reference_id != new_id {
                    // Remove old expression from indexes.
                    self.id_to_physical_expr.remove(&reference_id);
                    self.physical_expr_to_id.remove(&prev_expr);

                    // Update the goal member referencing expressions index
                    // for the new physical expr.
                    let old_refs = self
                        .goal_member_referencing_exprs_index
                        .remove(&GoalMemberId::PhysicalExpressionId(reference_id))
                        .unwrap_or_default();
                    self.goal_member_referencing_exprs_index
                        .entry(GoalMemberId::PhysicalExpressionId(new_id))
                        .or_default()
                        .extend(old_refs);

                    // Update the representative ID for the expression.
                    self.repr_physical_expr_id.merge(&reference_id, &new_id);

                    // This handles cascading effects where merging
                    // one expression affects others.
                    pending_dependencies.push_back(GoalMemberId::PhysicalExpressionId(new_id));

                    // Record the merge.
                    physical_expr_merges.push(MergePhysicalExprProduct {
                        new_physical_expr_id: new_id,
                        merged_physical_exprs: vec![reference_id],
                    });
                }
            }
        }

        // Consolidate the merge products to ensure no duplicates.
        self.consolidate_merge_physical_expr_products(physical_expr_merges)
            .await
    }
}
