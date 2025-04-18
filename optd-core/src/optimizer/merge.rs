#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use super::{
    Optimizer,
    tasks::{SourceTaskId, Task},
};
use crate::{
    cir::{
        Cost, GoalId, GoalMemberId, GroupId, ImplementationRule, LogicalExpressionId,
        TransformationRule,
    },
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
        // <I. Apply group merges>
        for group_merge in result.group_merges {
            // collect all the explore group tasks for merged groups.
            let mut explore_group_tasks = Vec::new();
            let all_exprs_by_group = group_merge.merged_groups;
            let new_repr_group_id = group_merge.new_repr_group_id;
            // Step 1: For each group that has an exploration task, we launch new subtasks based on subscription.
            // Note: It is fine to have duplication at this stage, we will remove duplicates later.
            for group_id in all_exprs_by_group.keys() {
                //
                let Some(explore_group_task_id) =
                    self.group_exploration_task_index.get(group_id).cloned()
                else {
                    continue;
                };
                explore_group_tasks.push((explore_group_task_id, *group_id));

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
            // END Step 1.
            // BEGIN Step 2: Merge the explore group tasks.
            let maybe_primary_task = match explore_group_tasks.as_slice() {
                [] => None, // No tasks exist, nothing to do.
                [(task_id, group_id)] => {
                    // Just one task exists - update its index.
                    if *group_id != new_repr_group_id {
                        self.group_exploration_task_index.remove(group_id);
                    }
                    self.group_exploration_task_index
                        .insert(new_repr_group_id, *task_id);

                    let mut primary_task = self.tasks.remove(task_id).unwrap().into_explore_group();
                    primary_task.group_id = new_repr_group_id;
                    Some(primary_task)
                }
                [(primary_task_id, primary_group_id), rest @ ..] => {
                    // Multiple tasks - merge them into the primary task.
                    let mut optimize_goal_task_ids = Vec::new();
                    let mut fork_logical_task_ids = Vec::new();
                    let mut transform_expr_task_ids = Vec::new();

                    for (task_id, group_id) in rest {
                        let task = self.tasks.remove(task_id).unwrap().into_explore_group();
                        self.group_exploration_task_index.remove(group_id);
                        optimize_goal_task_ids.extend(task.optimize_goal_out);
                        fork_logical_task_ids.extend(task.fork_logical_out);
                        transform_expr_task_ids.extend(task.transform_expr_in);
                    }

                    let mut primary_task = self
                        .tasks
                        .remove(primary_task_id)
                        .unwrap()
                        .into_explore_group();

                    for task_id in optimize_goal_task_ids.iter() {
                        let optimize_goal_task =
                            self.tasks.get_mut(task_id).unwrap().as_optimize_goal_mut();
                        optimize_goal_task.explore_group_in = *primary_task_id;
                    }
                    primary_task
                        .optimize_goal_out
                        .extend(optimize_goal_task_ids);

                    for task_id in fork_logical_task_ids.iter() {
                        let fork_logical_task =
                            self.tasks.get_mut(task_id).unwrap().as_fork_logical_mut();
                        fork_logical_task.explore_group_in = *primary_task_id;
                    }
                    primary_task.fork_logical_out.extend(fork_logical_task_ids);

                    for task_id in transform_expr_task_ids.iter() {
                        let transform_expr_task = self
                            .tasks
                            .get_mut(task_id)
                            .unwrap()
                            .as_transform_expression_mut();
                        transform_expr_task.explore_group_out = *primary_task_id;
                    }
                    primary_task
                        .transform_expr_in
                        .extend(transform_expr_task_ids);
                    primary_task.group_id = new_repr_group_id;

                    // remap the new_repr_group_id to point to the primary task.
                    self.group_exploration_task_index.remove(primary_group_id);
                    self.group_exploration_task_index
                        .insert(new_repr_group_id, *primary_task_id);
                    Some(primary_task)
                }
            };
            // END Step 2.
            let Some(mut primary_task) = maybe_primary_task else {
                continue;
            };
            // Step 3: Dedup the subscribers and publishers.

            // Dedup all transform expression.
            let mut transforms = HashMap::new();
            for task_id in primary_task.transform_expr_in.iter() {
                let task = self.tasks.get(task_id).unwrap().as_transform_expression();
                let repr_expr_id = self
                    .memo
                    .find_repr_logical_expr(task.logical_expr_id)
                    .await?;
                if let Some(old_task_id) =
                    transforms.insert((repr_expr_id, task.rule.clone()), *task_id)
                {
                    // Remove old task.
                    // TODO: Unlink subtree, recursively.
                    self.tasks.remove(&old_task_id);
                }
            }
            primary_task.transform_expr_in = transforms.values().cloned().collect();
        }

        // <II. Apply goal merges>
        for goal_merge in result.goal_merges {
            let mut optimize_goal_tasks = Vec::new();
            let all_exprs_by_goal = &goal_merge.merged_goals;
            let new_repr_goal_id = goal_merge.new_repr_goal_id;

            for (goal_id, current_goal_info) in all_exprs_by_goal.iter() {
                let Some(optimize_goal_task_id) =
                    self.goal_optimization_task_index.get(goal_id).cloned()
                else {
                    continue;
                };
                optimize_goal_tasks.push((optimize_goal_task_id, *goal_id));

                let (optimize_plan_task_ids, fork_costed_task_ids) = {
                    let optimize_goal_task = self
                        .tasks
                        .get_mut(&optimize_goal_task_id)
                        .unwrap()
                        .as_optimize_goal();
                    (
                        optimize_goal_task.optimize_plan_out.clone(),
                        optimize_goal_task.fork_costed_out.clone(),
                    )
                };
                let current_cost = current_goal_info.best_expr.as_ref().map(|(_, cost)| *cost);

                // TODO: could we reuse `handle_forward_result` logic?
                let members_from_others = all_exprs_by_goal
                    .iter()
                    .filter(|(id, _)| *id != goal_id)
                    .flat_map(|(_, goal_info)| goal_info.members.iter())
                    .cloned()
                    .collect::<HashSet<_>>();

                // Ensure there is a subtask for all the goal members from other goals.
                for member in members_from_others {
                    match member {
                        GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
                            let task_id = self
                                .ensure_cost_expression_task(
                                    physical_expr_id,
                                    Cost(f64::MAX),
                                    optimize_goal_task_id,
                                )
                                .await?;
                            let optimize_goal_task = self
                                .tasks
                                .get_mut(&optimize_goal_task_id)
                                .unwrap()
                                .as_optimize_goal_mut();
                            optimize_goal_task.add_cost_expr_in(task_id);
                        }
                        GoalMemberId::GoalId(goal_id) => {
                            let (task_id, _) = self
                                .ensure_optimize_goal_task(
                                    goal_id,
                                    SourceTaskId::OptimizeGoal(optimize_goal_task_id),
                                )
                                .await?;
                            let optimize_goal_task = self
                                .tasks
                                .get_mut(&optimize_goal_task_id)
                                .unwrap()
                                .as_optimize_goal_mut();
                            optimize_goal_task.add_optimize_goal_in(task_id);
                        }
                    }
                }

                let best_from_others = all_exprs_by_goal
                    .iter()
                    .filter(|(id, _)| *id != goal_id)
                    .filter_map(|(_, goal_info)| goal_info.best_expr)
                    .filter(|(_, cost)| current_cost.is_none_or(|current| *cost < current))
                    .fold(None, |acc, (expr_id, cost)| match acc {
                        None => Some((expr_id, cost)),
                        Some((_, acc_cost)) if cost < acc_cost => Some((expr_id, cost)),
                        Some(_) => acc,
                    });

                if let Some((best_expr_id, best_cost)) = best_from_others {
                    // Send to client of optimize plan.
                    for task_id in optimize_plan_task_ids.iter() {
                        let optimize_plan_task =
                            self.tasks.get(task_id).unwrap().as_optimize_plan();
                        self.emit_best_physical_plan(
                            optimize_plan_task.physical_plan_tx.clone(),
                            best_expr_id,
                        )
                        .await?;
                    }
                    // Lanuch continuations.
                    for task_id in fork_costed_task_ids.iter() {
                        let continue_with_costed_task_id = self
                            .create_continue_with_costed_task(best_expr_id, best_cost, *task_id)
                            .await?;
                        let fork_costed_task =
                            self.tasks.get_mut(task_id).unwrap().as_fork_costed_mut();
                        fork_costed_task.budget = best_cost;
                        fork_costed_task.add_continue_in(continue_with_costed_task_id);
                    }
                }
            }

            // TODO(yuchen): we should also dedup the implement expressions here, besides other stuff.
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
