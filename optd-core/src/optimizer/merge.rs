use super::{
    memo::{Memoize, MergeResult, MergedGoalInfo, MergedGroupInfo},
    Optimizer, TaskId,
};
use crate::{
    cir::{goal::GoalId, group::GroupId},
    error::Error,
};
use std::collections::HashMap;

impl<M: Memoize> Optimizer<M> {
    /// Helper method to handle different types of merge results.
    ///
    /// This method processes the results of group and goal merges, updating
    /// subscribers and tasks appropriately.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle.
    pub(super) async fn handle_merge_result(&mut self, result: MergeResult) -> Result<(), Error> {
        // First, handle all the group merges.
        for group_merge in result.group_merges {
            let all_exprs_by_group = group_merge.merged_groups;
            let new_repr_group_id = group_merge.new_repr_group_id;

            // 1. For each group, schedule expressions from all OTHER groups,
            // ignoring any potential duplicates due to merges for now.
            for (i, current_group_info) in all_exprs_by_group.iter().enumerate() {
                let other_groups_exprs: Vec<_> = all_exprs_by_group
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i) // Filter out the current group.
                    .flat_map(|(_, group_info)| &group_info.expressions)
                    .copied()
                    .collect();

                self.schedule_logical_continuations(
                    current_group_info.group_id,
                    &other_groups_exprs,
                );
            }

            // 2. Handle exploration tasks for the merged groups.
            self.merge_exploration_tasks(&all_exprs_by_group, new_repr_group_id)
                .await;

            // 3. Merge subscribers.
        }

        // Second, handle all the goal merges.
        for goal_merge in result.goal_merges {
            let all_exprs_by_goal = &goal_merge.merged_goals;

            // 1. For each goal, schedule the best expression from all OTHER goals only if it is
            // better than the current best expression for the goal.
            for (i, current_goal_info) in all_exprs_by_goal.iter().enumerate() {
                let current_cost = current_goal_info.best_expr.as_ref().map(|(_, cost)| cost);

                let best_from_others = all_exprs_by_goal
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i) // Filter out the current goal.
                    .filter_map(|(_, goal_info)| goal_info.best_expr)
                    .filter(|(_, cost)| current_cost.map_or(true, |current| cost < current))
                    .fold(None, |acc, (expr_id, cost)| match acc {
                        None => Some((expr_id, cost)),
                        Some((_, acc_cost)) if cost < acc_cost => Some((expr_id, cost)),
                        Some(_) => acc,
                    });

                if let Some((best_expr_id, best_cost)) = best_from_others {
                    self.schedule_optimized_continuations(
                        current_goal_info.goal_id,
                        best_expr_id,
                        best_cost,
                    );
                    self.egest_to_subscribers(current_goal_info.goal_id, best_expr_id)
                        .await?;
                }
            }

            // 2. Handling costing tasks for the merged goals.
            self.merge_optimization_tasks(&all_exprs_by_goal, goal_merge.new_repr_goal_id)
                .await;

            // 3. Merge subscribers.
        }

        // Third, launch the newly dirty stuff if needed.

        Ok(())
    }

    /// Helper method to merge exploration tasks for merged groups.
    ///
    /// # Parameters
    /// * `all_exprs_by_group` - All groups and their expressions that were merged.
    /// * `new_repr_group_id` - The new representative group ID.
    async fn merge_exploration_tasks(
        &mut self,
        all_exprs_by_group: &[MergedGroupInfo],
        new_repr_group_id: GroupId,
    ) {
        // Collect all task IDs associated with the merged groups.
        let exploring_tasks: Vec<_> = all_exprs_by_group
            .iter()
            .filter_map(|group_info| {
                self.group_exploration_task_index
                    .get(&group_info.group_id)
                    .copied()
                    .map(|task_id| (task_id, group_info.group_id))
            })
            .collect();

        match exploring_tasks.as_slice() {
            [] => return, // No tasks exist, nothing to do.

            [(task_id, group_id)] => {
                // Just one task exists - update its index.
                if *group_id != new_repr_group_id {
                    self.group_exploration_task_index.remove(group_id);
                }

                self.group_exploration_task_index
                    .insert(new_repr_group_id, *task_id);
            }

            [(primary_task_id, _), rest @ ..] => {
                // Multiple tasks - merge them into the primary task.
                let mut children_to_add = Vec::new();
                for (task_id, _) in rest {
                    let task = self.tasks.get(task_id).unwrap();
                    children_to_add.extend(task.children.clone());
                    self.tasks.remove(task_id);
                }

                let primary_task = self.tasks.get_mut(primary_task_id).unwrap();
                primary_task.children.extend(children_to_add);

                for group_info in all_exprs_by_group {
                    self.group_exploration_task_index
                        .remove(&group_info.group_id);
                }

                self.group_exploration_task_index
                    .insert(new_repr_group_id, *primary_task_id);
            }
        }
    }

    /// Helper method to merge optimization tasks for merged goals.
    async fn merge_optimization_tasks(
        &mut self,
        all_exprs_by_goal: &[MergedGoalInfo],
        new_repr_goal_id: GoalId,
    ) {
        // Collect all task IDs associated with the merged goals.
        let optimization_tasks: Vec<_> = all_exprs_by_goal
            .iter()
            .filter_map(|goal_info| {
                self.goal_optimization_task_index
                    .get(&goal_info.goal_id)
                    .copied()
                    .map(|task_id| (task_id, goal_info.goal_id))
            })
            .collect();

        match optimization_tasks.as_slice() {
            [] => return, // No tasks exist, nothing to do.

            [(task_id, goal_id)] => {
                // Just one task exists - update its index and kind.
                if *goal_id != new_repr_goal_id {
                    self.goal_optimization_task_index.remove(goal_id);
                }

                self.goal_optimization_task_index
                    .insert(new_repr_goal_id, *task_id);
            }

            [(primary_task_id, _), rest @ ..] => {
                // Multiple tasks - merge them into the primary task.
                let mut children_to_add = Vec::new();
                for (task_id, _) in rest {
                    let task = self.tasks.get(task_id).unwrap();
                    children_to_add.extend(task.children.clone());
                    self.tasks.remove(task_id);
                }

                let primary_task = self.tasks.get_mut(primary_task_id).unwrap();
                primary_task.children.extend(children_to_add);

                for goal_info in all_exprs_by_goal {
                    self.goal_optimization_task_index.remove(&goal_info.goal_id);
                }

                self.goal_optimization_task_index
                    .insert(new_repr_goal_id, *primary_task_id);
            }
        }
    }
}
