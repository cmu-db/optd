use super::{Optimizer, errors::OptimizeError, tasks::TaskId};
use crate::memo::{Memo, MergeGroupProduct, MergeProducts};
use std::collections::{HashMap, HashSet};

mod dedup;

impl<M: Memo> Optimizer<M> {
    /// Helper method to handle different types of merge results.
    ///
    /// This method processes the results of group and goal merges, updating the
    /// internal task graph accordingly. Specifically it:
    ///
    /// 1. TODO(Alexis): Document.
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

            if group_explore_tasks.is_empty() {
                continue; // No tasks exist, nothing to do.
            }

            for task_id in &group_explore_tasks {
                self.dedup_group_explore(*task_id).await?;
            }

            let (principal_task_id, secondary_task_ids) =
                group_explore_tasks.split_first().unwrap();

            let mut fork_logical_tasks = HashSet::new();
            let mut optimize_goal_tasks = HashSet::new();

            for &task_id in secondary_task_ids {
                let task = self.get_explore_group_task_mut(task_id).unwrap();
                let forks = std::mem::take(&mut task.fork_logical_out);
                fork_logical_tasks.extend(forks);
            }
            for &task_id in secondary_task_ids {
                let task = self.get_explore_group_task_mut(task_id).unwrap();
                let goals = std::mem::take(&mut task.optimize_goal_out);
                optimize_goal_tasks.extend(goals);
            }

            // TODO: LATER DO NOT TOUCH
        }

        Ok(())
    }
}
