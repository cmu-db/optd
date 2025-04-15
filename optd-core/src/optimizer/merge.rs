#![allow(dead_code)]

use std::collections::HashSet;

use super::Optimizer;
use crate::{
    cir::{GoalId, GroupId, ImplementationRule, LogicalExpressionId, TransformationRule},
    error::Error,
    memo::{Memoize, MergeResult, MergedGoalInfo},
    optimizer::tasks::TaskId,
};

impl<M: Memoize> Optimizer<M> {
    /// Helper method to handle different types of merge results.
    ///
    /// This method processes the results of group and goal merges, updating
    /// subscribers and tasks appropriately.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle.
    pub(super) async fn handle_merge_result(&mut self, result: MergeResult) -> Result<(), Error> {
        // ## Apply group merges:
        for group_merge in result.group_merges {
            let all_exprs_by_group = group_merge.merged_groups;
            let new_repr_group_id = group_merge.new_repr_group_id;
            // For each group that has an exploration task, we launch new subtasks based on subscription.
            // Note: It is fine to have duplication at this stage, we will remove duplicates later.
            for (group_id, _) in all_exprs_by_group.iter() {
                //
                let Some(explore_group_task_id) =
                    self.group_exploration_task_index.get(group_id).cloned()
                else {
                    continue;
                };

                let (optimize_goal_task_ids, fork_logical_task_ids) = {
                    let explore_group_task = self
                        .tasks
                        .get(&explore_group_task_id)
                        .unwrap()
                        .as_explore_group();

                    (
                        explore_group_task.optimize_goal_out.clone(),
                        explore_group_task.fork_logical_out.clone(),
                    )
                };

                let other_groups_exprs: Vec<_> = all_exprs_by_group
                    .iter()
                    .filter(|(id, _)| *id != group_id)
                    .flat_map(|(_, exprs)| exprs)
                    .cloned()
                    .collect();

                for logical_expr_id in other_groups_exprs {
                    let transformations = self.rule_book.get_transformations().to_vec();

                    // Create transformations for each logical expression from the other groups
                    let xform_expr_task_ids = self
                        .create_transformation_tasks(
                            logical_expr_id,
                            explore_group_task_id,
                            transformations,
                        )
                        .await?;

                    let explore_group_task = self
                        .get_task_mut(explore_group_task_id)
                        .as_explore_group_mut();
                    for task_id in xform_expr_task_ids {
                        explore_group_task.add_transform_expr_in(task_id);
                    }

                    // Create implementations for each logical expression from the other groups
                    // for each goal that depends on the group.
                    for optimize_goal_task_id in optimize_goal_task_ids.iter() {
                        let goal_id = {
                            let optimize_goal_task = self
                                .tasks
                                .get(optimize_goal_task_id)
                                .unwrap()
                                .as_optimize_goal();
                            optimize_goal_task.goal_id
                        };

                        let implementations = self.rule_book.get_implementations().to_vec();

                        let impl_expr_task_ids = self
                            .create_implementation_tasks(
                                logical_expr_id,
                                goal_id,
                                *optimize_goal_task_id,
                                implementations,
                            )
                            .await?;

                        let optimize_goal_task = self
                            .tasks
                            .get_mut(optimize_goal_task_id)
                            .unwrap()
                            .as_optimize_goal_mut();

                        for task_id in impl_expr_task_ids {
                            optimize_goal_task.add_implement_expr_in(task_id);
                        }
                    }

                    // Create continuation for each logical expression from the other groups
                    // at each fork point that depends on the group.
                    for fork_logical_task_id in fork_logical_task_ids.iter() {
                        let cont_with_logical_task_id = self
                            .create_continue_with_logical_task(
                                logical_expr_id,
                                *fork_logical_task_id,
                            )
                            .await?;
                        let fork_logical_task = self
                            .tasks
                            .get_mut(fork_logical_task_id)
                            .unwrap()
                            .as_fork_logical_mut();
                        fork_logical_task.add_continue_in(cont_with_logical_task_id);
                    }
                }
            }
        }

        Ok(())
    }

    async fn create_implementation_tasks(
        &mut self,
        expr: LogicalExpressionId,
        goal_id: GoalId,
        optimize_goal_out: TaskId,
        implementations: Vec<ImplementationRule>,
    ) -> Result<Vec<TaskId>, Error> {
        let mut task_ids = Vec::with_capacity(implementations.len());
        for rule in implementations {
            let impl_expr_task_id = self
                .create_implement_expression_task(rule, expr, goal_id, optimize_goal_out)
                .await?;
            task_ids.push(impl_expr_task_id);
        }
        Ok(task_ids)
    }

    async fn create_transformation_tasks(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        explore_group_out: TaskId,
        transformations: Vec<TransformationRule>,
    ) -> Result<Vec<TaskId>, Error> {
        let mut task_ids = Vec::with_capacity(transformations.len());
        for rule in transformations {
            let impl_expr_task_id = self
                .create_transform_expression_task(rule, logical_expr_id, explore_group_out)
                .await?;
            task_ids.push(impl_expr_task_id);
        }
        Ok(task_ids)
    }

    /// Helper method to merge exploration tasks for merged groups.
    ///
    /// # Parameters
    /// * `all_exprs_by_group` - All groups and their expressions that were merged.
    /// * `new_repr_group_id` - The new representative group ID.
    async fn merge_exploration_tasks(
        &mut self,
        _all_exprs_by_group: &[(GroupId, Vec<LogicalExpressionId>)],
        _new_repr_group_id: GroupId,
    ) {
        // // Collect all task IDs associated with the merged groups.
        // let exploring_tasks: Vec<_> = all_exprs_by_group
        //     .iter()
        //     .filter_map(|group_info| {
        //         self.group_exploration_task_index
        //             .get(&group_info.group_id)
        //             .copied()
        //             .map(|task_id| (task_id, group_info.group_id))
        //     })
        //     .collect();

        // match exploring_tasks.as_slice() {
        //     [] => (), // No tasks exist, nothing to do.

        //     [(task_id, group_id)] => {
        //         // Just one task exists - update its index.
        //         if *group_id != new_repr_group_id {
        //             self.group_exploration_task_index.remove(group_id);
        //         }

        //         self.group_exploration_task_index
        //             .insert(new_repr_group_id, *task_id);
        //     }

        //     [(primary_task_id, _), rest @ ..] => {
        //         // Multiple tasks - merge them into the primary task.
        //         let mut children_to_add = Vec::new();
        //         for (task_id, _) in rest {
        //             let task = self.tasks.get(task_id).unwrap();
        //             children_to_add.extend(task.children.clone());
        //             self.tasks.remove(task_id);
        //         }

        //         let primary_task = self.tasks.get_mut(primary_task_id).unwrap();
        //         primary_task.children.extend(children_to_add);

        //         for group_info in all_exprs_by_group {
        //             self.group_exploration_task_index
        //                 .remove(&group_info.group_id);
        //         }

        //         self.group_exploration_task_index
        //             .insert(new_repr_group_id, *primary_task_id);
        //     }
        // }
    }

    /// Helper method to merge optimization tasks for merged goals.
    async fn merge_optimization_tasks(
        &mut self,
        _all_exprs_by_goal: &[MergedGoalInfo],
        _new_repr_goal_id: GoalId,
    ) {
        // // Collect all task IDs associated with the merged goals.
        // let optimization_tasks: Vec<_> = all_exprs_by_goal
        //     .iter()
        //     .filter_map(|goal_info| {
        //         self.goal_optimization_task_index
        //             .get(&goal_info.goal_id)
        //             .copied()
        //             .map(|task_id| (task_id, goal_info.goal_id))
        //     })
        //     .collect();

        // match optimization_tasks.as_slice() {
        //     [] => (), // No tasks exist, nothing to do.

        //     [(task_id, goal_id)] => {
        //         // Just one task exists - update its index and kind.
        //         if *goal_id != new_repr_goal_id {
        //             self.goal_optimization_task_index.remove(goal_id);
        //         }

        //         self.goal_optimization_task_index
        //             .insert(new_repr_goal_id, *task_id);
        //     }

        //     [(primary_task_id, _), rest @ ..] => {
        //         // Multiple tasks - merge them into the primary task.
        //         let mut children_to_add = Vec::new();
        //         for (task_id, _) in rest {
        //             let task = self.tasks.get(task_id).unwrap();
        //             children_to_add.extend(task.children.clone());
        //             self.tasks.remove(task_id);
        //         }

        //         let primary_task = self.tasks.get_mut(primary_task_id).unwrap();
        //         primary_task.children.extend(children_to_add);

        //         for goal_info in all_exprs_by_goal {
        //             self.goal_optimization_task_index.remove(&goal_info.goal_id);
        //         }

        //         self.goal_optimization_task_index
        //             .insert(new_repr_goal_id, *primary_task_id);
        //     }
        // }
    }
}
