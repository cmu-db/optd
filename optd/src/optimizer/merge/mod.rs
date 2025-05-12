use super::{Optimizer, errors::OptimizeError};
use crate::memo::{Memo, MergeGroupProduct, MergeProducts};

mod helpers;

impl<M: Memo> Optimizer<M> {
    /// Helper method to handle different types of merge results.
    ///
    /// This method processes the results of group and goal merges, updating the
    /// internal task graph accordingly. Specifically it:
    ///
    /// TODO(Alexis): Document. Basically: dedup, update, and consolidate.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle.
    pub(super) async fn handle_merge_result(
        &mut self,
        result: MergeProducts,
    ) -> Result<(), OptimizeError> {
        for MergeGroupProduct {
            new_group_id,
            merged_groups,
        } in result.group_merges
        {
            // For each merged group, get all group exploration tasks.
            // We don't need to check for the new group ID since it is guaranteed to be new.
            let group_explore_tasks: Vec<_> = merged_groups
                .iter()
                .filter_map(|group_id| self.group_exploration_task_index.get(group_id).copied())
                .collect();

            if !group_explore_tasks.is_empty() {
                let all_logical_exprs = self
                    .memo
                    .get_all_logical_exprs(new_group_id)
                    .await
                    .map_err(OptimizeError::MemoError)?;

                let (principal_task_id, secondary_task_ids) =
                    group_explore_tasks.split_first().unwrap();

                for task_id in &group_explore_tasks {
                    // Step 1: Start by deduplicating all transform & continue tasks given
                    // potentially merged logical expressions.
                    self.dedup_group_explore(*task_id).await?;
                    // Step 2: Send *new* logical expressions to each task.
                    let is_principal = task_id == principal_task_id;
                    self.update_group_explore(*task_id, &all_logical_exprs, is_principal)
                        .await?;
                }

                // Step 3: Consolidate all dependent tasks into the new "representative" task.
                self.consolidate_group_explore(*principal_task_id, secondary_task_ids)
                    .await;

                // Step 4: Set the index to point to the new representative task.
                self.group_exploration_task_index
                    .insert(new_group_id, *principal_task_id);
            }
        }

        Ok(())
    }
}
