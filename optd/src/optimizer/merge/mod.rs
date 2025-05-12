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
        // Handle all the group merges.
        for MergeGroupProduct {
            new_group_id,
            merged_groups,
        } in result.group_merges
        {
            // Use index to get all group explorations.
            let group_explore_tasks: Vec<_> = merged_groups
                .iter()
                .filter_map(|group_id| self.group_exploration_task_index.get(group_id).copied())
                .collect();

            if !group_explore_tasks.is_empty() {
                // Step 1: Start by deduplicating all transform & continue tasks given
                // potentially merged logical expressions.
                for task_id in &group_explore_tasks {
                    self.dedup_group_explore(*task_id).await?;
                }

                // Step 2: Send *new* logical expressions to each task.

                // Step 3: Consolidate all dependent tasks into the new "representative" task.
                let (principal_task_id, secondary_task_ids) =
                    group_explore_tasks.split_first().unwrap();
                self.consolidate_group_explore(*principal_task_id, secondary_task_ids)
                    .await;

                // Step 4: Update the index to point to the new representative task.
                self.group_exploration_task_index
                    .insert(new_group_id, *principal_task_id);
            }
        }

        Ok(())
    }
}
