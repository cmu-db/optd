use super::Optimizer;
use crate::memo::{Memo, MergeGroupProduct, MergeProducts};

mod helpers;

impl<M: Memo> Optimizer<M> {
    /// Processes merge results by updating the task graph to reflect merges in the memo.
    ///
    /// When groups are merged in the memo, multiple exploration tasks may now refer to
    /// the same underlying group. This method handles the task graph updates required
    /// by such merges. For each merged group, it:
    ///
    /// 1. **Deduplicates**: For each affected exploration task, removes duplicate logical
    ///    expressions that now map to the same representative, cleaning up or updating
    ///    related transform and continuation tasks.
    ///
    /// 2. **Updates**: Sends any new logical expressions to each task, creating appropriate
    ///    transform tasks (for the principal task) and continuation tasks (for all tasks).
    ///
    /// 3. **Consolidates**: Merges all secondary tasks into a principal task by transferring
    ///    their dependencies and updating references, ensuring a clean 1:1 mapping between
    ///    groups and exploration tasks.
    ///
    /// 4. **Re-indexes**: Updates the group exploration index to point to the principal task
    ///    for the new group ID.
    ///
    /// After processing, each merged group will have exactly one exploration task associated
    /// with it, containing all logical expressions from the original groups with no duplicates.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle, containing information about merged groups.
    ///
    /// # Returns
    /// * `Result<(), OptimizeError>` - Success or an error that occurred during processing.
    pub(super) async fn handle_merge_result(
        &mut self,
        result: MergeProducts,
    ) -> Result<(), M::MemoError> {
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
                let all_logical_exprs = self.memo.get_all_logical_exprs(new_group_id).await?;

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
