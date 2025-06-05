use super::Optimizer;
use crate::memo::{Memo, MergeGoalProduct, MergeGroupProduct, MergeProducts};

mod helpers;

impl<M: Memo> Optimizer<M> {
    /// Processes merge results by updating the task graph to reflect merges in the memo.
    ///
    /// This function handles both group merges and goal merges by delegating to specialized
    /// handlers for each type of merge.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle, containing information about merged groups and goals.
    ///
    /// # Returns
    /// * `Result<(), OptimizeError>` - Success or an error that occurred during processing.
    #[tracing::instrument(level = "info", skip(self, result), target = "optd::optimizer::merge")]
    pub(super) async fn handle_merge_result(
        &mut self,
        result: MergeProducts,
    ) -> Result<(), M::MemoError> {
        tracing::debug!(
            target: "optd::optimizer::merge",
            "Handling {} group merges and {} goal merges", result.group_merges.len(), result.goal_merges.len()
        );
        self.handle_group_merges(result.group_merges).await?;
        self.handle_goal_merges(&result.goal_merges).await
    }

    /// Handles merges of logical groups by updating the task graph.
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
    ///    transform (for the principal task), implement and continuation tasks (for all tasks).
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
    /// * `group_merges` - A slice of group merge products to handle.
    ///
    /// # Returns
    /// * `Result<(), OptimizeError>` - Success or an error that occurred during processing.
    #[tracing::instrument(level = "debug", skip_all, name = "handle_group_merges")]
    async fn handle_group_merges(
        &mut self,
        group_merges: Vec<MergeGroupProduct>,
    ) -> Result<(), M::MemoError> {
        for MergeGroupProduct {
            new_group_id,
            merged_groups,
        } in group_merges
        {
            tracing::debug!(target: "optd::optimizer::merge", ?new_group_id, num_merged_groups = merged_groups.len(), "Processing group merge product");
            // For each merged group, get all group exploration tasks.
            // We don't need to check for the new group ID since it is guaranteed to be new.
            let group_explore_tasks: Vec<_> = merged_groups
                .iter()
                .filter_map(|group_id| self.group_exploration_task_index.get(group_id).copied())
                .collect();

            if !group_explore_tasks.is_empty() {
                tracing::debug!(target: "optd::optimizer::merge", ?new_group_id, num_related_explore_tasks = group_explore_tasks.len(), "Found related group exploration tasks");
                let all_logical_exprs = self.memo.get_all_logical_exprs(new_group_id).await?;

                let (principal_task_id, secondary_task_ids) =
                    group_explore_tasks.split_first().unwrap();

                for task_id in &group_explore_tasks {
                    // Step 1: Start by deduplicating all transform, implement & continue tasks
                    // given potentially merged logical expressions.
                    self.dedup_tasks(*task_id).await?;
                    // Step 2: Send *new* logical expressions to each task.
                    let is_principal = task_id == principal_task_id;
                    tracing::debug!(target: "optd::optimizer::merge", task_id = ?task_id, is_principal, "Updating task with new expressions from merged group");
                    self.update_tasks(*task_id, &all_logical_exprs, is_principal)
                        .await?;
                }

                // Step 3: Consolidate all dependent tasks into the new "representative" task.
                tracing::debug!(target: "optd::optimizer::merge", ?principal_task_id, num_secondary_tasks = secondary_task_ids.len(), "Consolidating group exploration tasks");
                self.consolidate_group_explore(*principal_task_id, secondary_task_ids)
                    .await;

                // Step 4: Set the index to point to the new representative task.
                self.group_exploration_task_index
                    .insert(new_group_id, *principal_task_id);
            }
        }

        Ok(())
    }

    /// Handles merges of optimization goals by updating the task graph.
    ///
    /// When goals are merged in the memo, multiple optimization tasks may now refer to
    /// the same underlying goal. This method handles the task graph updates required
    /// by such merges. For each merged goal, it:
    ///
    /// 1. **Consolidates**: Merges all secondary tasks into a principal task by transferring
    ///    their dependencies (incoming and outgoing optimization tasks and plan tasks) and
    ///    updating references, ensuring a clean 1:1 mapping between goals and optimization tasks.
    ///
    /// 2. **Re-indexes**: Updates the goal optimization index to point to the principal task
    ///    for the new goal ID.
    ///
    /// After processing, each merged goal will have exactly one optimization task associated
    /// with it, with all dependencies properly redirected to it.
    ///
    /// # Parameters
    /// * `goal_merges` - A slice of goal merge products to handle.
    ///
    /// # Returns
    /// * `Result<(), OptimizeError>` - Success or an error that occurred during processing.
    #[tracing::instrument(level = "debug", skip_all, name = "handle_goal_merges")]
    async fn handle_goal_merges(
        &mut self,
        goal_merges: &[MergeGoalProduct],
    ) -> Result<(), M::MemoError> {
        for MergeGoalProduct {
            new_goal_id,
            merged_goals,
        } in goal_merges
        {
            tracing::debug!(target: "optd::optimizer::merge", ?new_goal_id, num_merged_goals = merged_goals.len(), "Processing goal merge product");
            // For each merged goal, get all goal optimization tasks.
            // We don't need to check for the new goal ID since it is guaranteed to be new.
            let goal_optimize_tasks: Vec<_> = merged_goals
                .iter()
                .filter_map(|goal_id| self.goal_optimization_task_index.get(goal_id).copied())
                .collect();

            if !goal_optimize_tasks.is_empty() {
                tracing::debug!(target: "optd::optimizer::merge", ?new_goal_id, num_related_optimize_tasks = goal_optimize_tasks.len(), "Found related goal optimization tasks");
                let (principal_task_id, secondary_task_ids) =
                    goal_optimize_tasks.split_first().unwrap();

                // *NOTE*: Deduplication and updates of implementation tasks have already been
                // handled in the `handle_group_merges` method, so we don't need to do it here.

                // Step 1: Consolidate all dependent tasks into the new "representative" task.
                tracing::debug!(target: "optd::optimizer::merge", ?principal_task_id, num_secondary_tasks = secondary_task_ids.len(), "Consolidating goal optimization tasks");
                self.consolidate_goal_optimize(*principal_task_id, secondary_task_ids)
                    .await;

                // Step 2: Set the index to point to the new representative task.
                self.goal_optimization_task_index
                    .insert(*new_goal_id, *principal_task_id);
            }
        }

        Ok(())
    }
}
