use super::MemoryMemo;
use crate::{
    cir::{GroupId, LogicalExpression, LogicalExpressionId},
    memo::{
        Materialize, Memo, MemoError, MergeGroupProduct, MergeProducts, Representative,
        error::MemoResult, memory::GroupInfo,
    },
};
use std::collections::{HashMap, HashSet};

impl MemoryMemo {
    pub(super) fn next_shared_id(&mut self) -> i64 {
        let id = self.next_shared_id;
        self.next_shared_id += 1;
        id
    }

    /// Gets the set of logical expression IDs that reference a group,
    /// mapped to their representatives.
    fn get_referencing_expr_set(&mut self, group_id: GroupId) -> HashSet<LogicalExpressionId> {
        self.group_referencing_exprs_index
            .remove(&group_id)
            .unwrap_or_default()
            .iter()
            .map(|id| self.repr_logical_expr_id.find(id))
            .collect()
    }

    /// Merges a pair of groups, creating a new representative group that combines their expressions.
    ///
    /// In the memo structure, when two groups are found to be equivalent, we need to create
    /// a new representative group that combines their expressions and properties. This function
    /// handles that core operation.
    ///
    /// # Algorithm
    ///
    /// The merging process involves several steps:
    /// 1. Create a new group ID to serve as the new representative.
    /// 2. Verify that the logical properties of both groups match (they should for equivalent groups).
    /// 3. Combine all the logical expressions from both groups.
    /// 4. Create a new group info entry with the combined expressions.
    /// 5. Update the union-find structure to point both original groups to the new representative.
    /// 6. Clean up the old group info entries.
    /// 7. Update the logical expression to group mappings.
    ///
    /// # Parameters
    /// * `group_id_1` - ID of the first group to merge.
    /// * `group_id_2` - ID of the second group to merge.
    ///
    /// # Returns
    /// The new representative group ID and a record of the merge operation.
    pub(super) async fn merge_group_pair(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<(GroupId, MergeGroupProduct)> {
        // Create new group with a fresh ID.
        let new_group_id = GroupId(self.next_shared_id());

        // Get group info for both groups.
        let group_info_1 = self
            .group_info
            .get(&group_id_1)
            .ok_or(MemoError::GroupNotFound(group_id_1))?;
        let group_info_2 = self
            .group_info
            .get(&group_id_2)
            .ok_or(MemoError::GroupNotFound(group_id_2))?;

        // Verify logical properties match (should be the case for equivalent groups)
        // Unless there is a bug in the rules.
        debug_assert!(group_info_1.logical_properties == group_info_2.logical_properties);

        // Combine expressions from both groups.
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

        // Update union-find structure.
        self.repr_group_id.merge(&group_id_1, &new_group_id);
        self.repr_group_id.merge(&group_id_2, &new_group_id);

        // Clean up old group info.
        self.group_info.remove(&group_id_1);
        self.group_info.remove(&group_id_2);

        // Update logical expression to group index.
        self.group_info[&new_group_id]
            .expressions
            .iter()
            .for_each(|&expr_id| {
                self.logical_id_to_group_index.insert(expr_id, new_group_id);
            });

        // Create merge product record
        let merge_product = MergeGroupProduct {
            new_repr_group_id: new_group_id,
            merged_groups: vec![group_id_1, group_id_2],
        };

        Ok((new_group_id, merge_product))
    }

    /// Handles changes to an expression after remapping.
    ///
    /// When an expression is remapped due to groups it references being merged,
    /// various data structures need to be updated. This function handles those
    /// updates and identifies any new group merges that are needed.
    ///
    /// # Algorithm
    ///
    /// The process involves several steps:
    /// 1. Remove the old expression from the relevant data structures.
    /// 2. Identify the groups that contain the old and new expressions.
    /// 3. Update the group info to replace the old expression with the new one.
    /// 4. Update the logical expression to group index.
    /// 5. Update the union-find structure to point the old expression ID to the new one.
    /// 6. Check if the old and new expressions are in different groups.
    /// 7. If they are, return those groups as a pair that needs to be merged.
    ///
    /// # Parameters
    /// * `old_id` - ID of the expression before remapping.
    /// * `new_id` - ID of the expression after remapping.
    /// * `prev_expr` - The expression before remapping.
    ///
    /// # Returns
    /// An Option containing a pair of groups to merge, if the old and new expressions
    /// are in different groups.
    pub(super) async fn handle_expression_change(
        &mut self,
        old_id: LogicalExpressionId,
        new_id: LogicalExpressionId,
        prev_expr: LogicalExpression,
    ) -> MemoResult<Option<(GroupId, GroupId)>> {
        // Remove old expression from data structures.
        self.id_to_logical_expr.remove(&old_id);
        self.logical_expr_to_id.remove(&prev_expr);

        // Get groups for old and new expressions.
        let old_group_opt = self.find_logical_expr_group(old_id).await?;
        let new_group_opt = self.find_logical_expr_group(new_id).await?;

        // Update group info if old expression was in a group.
        if let Some(old_group) = old_group_opt {
            let group_info = self
                .group_info
                .get_mut(&old_group)
                .ok_or(MemoError::GroupNotFound(old_group))?;

            // Replace old expression with new in the group.
            group_info.expressions.remove(&old_id);
            group_info.expressions.insert(new_id);

            // Update logical_id_to_group_index.
            self.logical_id_to_group_index.remove(&old_id);
            if new_group_opt.is_none() {
                self.logical_id_to_group_index.insert(new_id, old_group);
            }
        }

        // Update representative ID for the expression.
        self.repr_logical_expr_id.merge(&old_id, &new_id);

        // If the new and old expressions are in different groups, they need to be merged.
        match (new_group_opt, old_group_opt) {
            (Some(new_group), Some(old_group)) if new_group != old_group => {
                Ok(Some((new_group, old_group)))
            }
            _ => Ok(None),
        }
    }

    /// Processes expressions that reference the merged groups.
    ///
    /// When groups are merged, expressions that reference them need to be updated.
    /// This function handles that update process and identifies any new equivalences
    /// that result.
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
    ///
    /// # Parameters
    /// * `group_id_1` - ID of the first merged group.
    /// * `group_id_2` - ID of the second merged group.
    /// * `new_group_id` - ID of the new representative group.
    ///
    /// # Returns
    /// A vector of group pairs that need to be merged due to new equivalences.
    pub(super) async fn process_referencing_expressions(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
        new_group_id: GroupId,
    ) -> MemoResult<Vec<(GroupId, GroupId)>> {
        // Collect expressions referencing the merged groups.
        let expr_set_1 = self.get_referencing_expr_set(group_id_1);
        let expr_set_2 = self.get_referencing_expr_set(group_id_2);

        // Combine into a single set.
        let mut referenced_exprs = expr_set_1;
        referenced_exprs.extend(expr_set_2);

        // Update the index for the new group.
        self.group_referencing_exprs_index
            .insert(new_group_id, referenced_exprs.clone());

        let mut new_pending_merges = Vec::new();

        // Process each expression that referenced the merged groups.
        for reference_id in referenced_exprs {
            // Remap the expression to use updated group references.
            let prev_expr = self.materialize_logical_expr(reference_id).await?;
            let new_expr = self.remap_logical_expr(&prev_expr).await?;
            let new_id = self.get_logical_expr_id(&new_expr).await?;

            if new_id != reference_id {
                // If remapping created a different expression, handle the changes.
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
    /// This function takes a list of individual merge operations and consolidates them
    /// into a complete picture of which groups were merged into each final representative.
    /// It handles cases where past representatives themselves are merged into newer ones.
    ///
    /// # Parameters
    /// * `merge_operations` - List of all individual merge operations.
    ///
    /// # Returns
    /// Consolidated merge results.
    pub(super) async fn consolidate_merge_results(
        &self,
        merge_operations: Vec<MergeGroupProduct>,
    ) -> MemoResult<MergeProducts> {
        // Collect operations into a map from representative to all merged groups.
        let mut consolidated_map: HashMap<GroupId, HashSet<GroupId>> = HashMap::new();

        for op in merge_operations {
            let current_repr = self.find_repr_group_id(op.new_repr_group_id).await?;

            consolidated_map
                .entry(current_repr)
                .or_default()
                .extend(op.merged_groups.iter().copied());

            if op.new_repr_group_id != current_repr {
                consolidated_map
                    .entry(current_repr)
                    .or_default()
                    .insert(op.new_repr_group_id);
            }
        }

        // Build the final list of merge products from the consolidated map.
        let group_merges = consolidated_map
            .into_iter()
            .filter_map(|(repr, groups)| {
                (!groups.is_empty()).then(|| MergeGroupProduct {
                    new_repr_group_id: repr,
                    merged_groups: groups.into_iter().collect(),
                })
            })
            .collect();

        Ok(MergeProducts {
            group_merges,
            goal_merges: vec![],
        })
    }
}
