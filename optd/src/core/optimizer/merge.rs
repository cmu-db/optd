#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use super::{Optimizer, tasks::Task};
use crate::{
    core::cir::{GoalId, GroupId, ImplementationRule, LogicalExpressionId, TransformationRule},
    core::error::Error,
    core::memo::{Memoize, MergeProducts},
    core::optimizer::tasks::TaskId,
};

impl<M: Memoize> Optimizer<M> {
    /// Recursively deletes tasks that are no longer needed.
    /// Confirms before deletion that the task is not subscribed to by any other task by checking if the parent tasks (the outs) exist in the task index.
    /// If the parent task is not found in the task index, then the given task is safely deleted. Else, we do not delete the task.
    pub(super) fn try_delete_task(&mut self, task_id: TaskId) {
        if let Some(task) = self.tasks.get(&task_id) {
            // Check if the task is subscribed to by any other task. If yes, then return.
            match task {
                Task::ExploreGroup(task) => {
                    for id in task.fork_logical_out.iter() {
                        if self.tasks.contains_key(id) {
                            return;
                        }
                    }
                    for id in task.optimize_goal_out.iter() {
                        if self.tasks.contains_key(id) {
                            return;
                        }
                    }
                }
                Task::TransformExpression(task) => {
                    if self.tasks.contains_key(&task.explore_group_out) {
                        return;
                    }
                }
                Task::ForkLogical(task) => {
                    if self.tasks.contains_key(&task.out) {
                        return;
                    }
                }
                Task::OptimizePlan(_) => {
                    // panic!("OptimizePlan task should not be deleted");
                    // TODO(Sarvesh) I guess we can always delete an optimize plan task as it has not subscribers
                }
                Task::OptimizeGoal(task) => {
                    for id in task.optimize_goal_out.iter() {
                        if self.tasks.contains_key(id) {
                            return;
                        }
                    }
                    for id in task.optimize_plan_out.iter() {
                        if self.tasks.contains_key(id) {
                            return;
                        }
                    }
                    for id in task.fork_costed_out.iter() {
                        if self.tasks.contains_key(id) {
                            return;
                        }
                    }
                }
                Task::ImplementExpression(task) => {
                    if self.tasks.contains_key(&task.optimize_goal_out) {
                        return;
                    }
                }
                Task::CostExpression(task) => {
                    for id in task.optimize_goal_out.iter() {
                        if self.tasks.contains_key(id) {
                            return;
                        }
                    }
                }
                Task::ContinueWithLogical(task) => {
                    if self.tasks.contains_key(&task.fork_out) {
                        return;
                    }
                }
                Task::ForkCosted(task) => {
                    if self.tasks.contains_key(&task.out) {
                        return;
                    }
                }
                Task::ContinueWithCosted(task) => {
                    if self.tasks.contains_key(&task.fork_out) {
                        return;
                    }
                }
            }

            // No subscribers, safe to delete.
            let task = self.tasks.remove(&task_id).unwrap();

            // Try to delete the publisher tasks.
            match task {
                Task::ExploreGroup(task) => {
                    self.group_exploration_task_index.remove(&task.group_id);
                    for id in task.transform_expr_in {
                        self.try_delete_task(id);
                    }
                }
                Task::TransformExpression(task) => {
                    if let Some(id) = task.fork_in {
                        self.try_delete_task(id);
                    }
                }
                Task::ForkLogical(task) => {
                    for id in task.continue_ins {
                        self.try_delete_task(id);
                    }
                    self.try_delete_task(task.explore_group_in);
                }
                Task::OptimizePlan(optimize_plan_task) => {
                    self.try_delete_task(optimize_plan_task.optimize_goal_in);
                }
                Task::OptimizeGoal(task) => {
                    self.goal_optimization_task_index.remove(&task.goal_id);
                    self.try_delete_task(task.explore_group_in);
                    for id in task.optimize_goal_in {
                        self.try_delete_task(id);
                    }
                    for id in task.implement_expression_in {
                        self.try_delete_task(id);
                    }
                    for id in task.cost_expression_in {
                        self.try_delete_task(id);
                    }
                }
                Task::ImplementExpression(task) => {
                    if let Some(id) = task.fork_in {
                        self.try_delete_task(id);
                    }
                }
                Task::CostExpression(task) => {
                    if let Some(id) = task.fork_in {
                        self.try_delete_task(id);
                    }
                }
                Task::ContinueWithLogical(task) => {
                    if let Some(id) = task.fork_in {
                        self.try_delete_task(id);
                    }
                }
                Task::ForkCosted(task) => {
                    for id in task.continue_ins {
                        self.try_delete_task(id);
                    }
                    self.try_delete_task(task.optimize_goal_in);
                }
                Task::ContinueWithCosted(task) => {
                    if let Some(id) = task.fork_in {
                        self.try_delete_task(id);
                    }
                }
            }
        }
    }

    /// Helper method to handle different types of merge results.
    ///
    /// This method processes the results of group and goal merges, updating
    /// subscribers and tasks appropriately.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle.
    pub(super) async fn handle_merge_result(&mut self, result: MergeProducts) -> Result<(), Error> {
        // <I. Apply group merges>
        for group_merge in result.group_merges {
            let repr_group_id = group_merge.new_repr_group_id;
            let non_repr_group_id = group_merge.old_non_repr_group_id;
            let all_exprs = group_merge.all_exprs_in_merged_group;
            let new_repr_group_exprs = group_merge.new_repr_group_exprs;
            let old_non_repr_group_exprs = group_merge.old_non_repr_group_exprs;

            let repr_group_task_id = self.group_exploration_task_index.get(&repr_group_id);
            let non_repr_group_task_id = self.group_exploration_task_index.get(&non_repr_group_id);
            if repr_group_task_id.is_none() && non_repr_group_task_id.is_none() {
                // There is no task for the new or old representative group
                // So, we don't have any subscribers to notify or anything to move.
                // This merge does not change anything for the task graph.
                continue;
            }

            if repr_group_task_id.is_none() {
                // TODO(Sarvesh): we need to create a new explore group task for the new representative group.
                self.create_explore_group_task(repr_group_id, None).await?;
            }
            let repr_group_task_id = &self
                .group_exploration_task_index
                .get(&repr_group_id)
                .unwrap()
                .clone();

            // we need to forward all the results from the non-repr group to subscribers of the repr group and also launch the transform expr tasks.
            let (optimize_goal_task_ids, fork_logical_task_ids, repr_group_transform_expr_task_ids) = {
                let repr_task = self
                    .tasks
                    .get_mut(repr_group_task_id)
                    .unwrap()
                    .as_explore_group_mut();
                (
                    repr_task.optimize_goal_out.clone(),
                    repr_task.fork_logical_out.clone(),
                    repr_task.transform_expr_in.clone(),
                )
            };

            let exprs_not_seen_by_repr_group = all_exprs
                .difference(&new_repr_group_exprs)
                .cloned()
                .collect::<HashSet<LogicalExpressionId>>();

            for logical_expr_id in exprs_not_seen_by_repr_group {
                // Forward the logical expression to all the outs
                // Launch the implementation tasks
                for optimize_goal_task_id in optimize_goal_task_ids.iter() {
                    let goal_id = {
                        let optimize_goal_task = self
                            .tasks
                            .get_mut(optimize_goal_task_id)
                            .unwrap()
                            .as_optimize_goal_mut();
                        optimize_goal_task.explore_group_in = *repr_group_task_id;
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
                // Launch the fork logical transformation tasks
                for fork_logical_task_id in fork_logical_task_ids.iter() {
                    let cont_with_logical_task_id = self
                        .create_continue_with_logical_task(logical_expr_id, *fork_logical_task_id)
                        .await?;
                    let fork_logical_task = self
                        .tasks
                        .get_mut(fork_logical_task_id)
                        .unwrap()
                        .as_fork_logical_mut();
                    fork_logical_task.explore_group_in = *repr_group_task_id;
                    fork_logical_task.add_continue_in(cont_with_logical_task_id);
                }
            }

            let non_repr_group_task_id = self.group_exploration_task_index.get(&non_repr_group_id);
            if non_repr_group_task_id.is_none() {
                // There is no task for the non-representative group
                // So we don't need to forward any results to this group's subscribers, as there are no subscribers to this group.
                continue;
            }
            let non_repr_group_task_id = &non_repr_group_task_id.unwrap().clone();

            // TODO(Sarvesh): we need to forward all the results from the repr group to subscribers of the non-repr group.
            let (
                optimize_goal_task_ids,
                fork_logical_task_ids,
                non_repr_group_transform_expr_task_ids,
            ) = {
                let non_repr_group_task = self
                    .tasks
                    .get_mut(non_repr_group_task_id)
                    .unwrap()
                    .as_explore_group_mut();
                (
                    non_repr_group_task.optimize_goal_out.clone(),
                    non_repr_group_task.fork_logical_out.clone(),
                    non_repr_group_task.transform_expr_in.clone(),
                )
            };

            let exprs_not_seen_by_non_repr_group = all_exprs
                .difference(&old_non_repr_group_exprs)
                .cloned()
                .collect::<HashSet<LogicalExpressionId>>();

            for logical_expr_id in exprs_not_seen_by_non_repr_group {
                // Forward the logical expression to all the outs
                // Launch the implementation tasks
                for optimize_goal_task_id in optimize_goal_task_ids.iter() {
                    let goal_id = {
                        let optimize_goal_task = self
                            .tasks
                            .get_mut(optimize_goal_task_id)
                            .unwrap()
                            .as_optimize_goal_mut();
                        optimize_goal_task.explore_group_in = *repr_group_task_id;
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
                // Launch the fork logical transformation tasks
                for fork_logical_task_id in fork_logical_task_ids.iter() {
                    let cont_with_logical_task_id = self
                        .create_continue_with_logical_task(logical_expr_id, *fork_logical_task_id)
                        .await?;
                    let fork_logical_task = self
                        .tasks
                        .get_mut(fork_logical_task_id)
                        .unwrap()
                        .as_fork_logical_mut();
                    fork_logical_task.explore_group_in = *repr_group_task_id;
                    fork_logical_task.add_continue_in(cont_with_logical_task_id);
                }
            }

            // Now let's handle all the ins! The children! The transform expression tasks!
            // We have two set of tasks. For the logical expression in the union of those sets,
            // there can be a unique and repr transform expression task, or a unique and non-repr transform expression task, or both.
            // So, what we do is, we first look for duplicates in the union of the two sets.

            let mut exprs_to_trans_tasks = HashMap::new();
            for task_id in repr_group_transform_expr_task_ids
                .iter()
                .chain(non_repr_group_transform_expr_task_ids.iter())
            {
                let task = self.tasks.get(task_id).unwrap().as_transform_expression();
                let logical_expr_id = task.logical_expr_id;
                let repr_logical_expr_id =
                    self.memo.find_repr_logical_expr(logical_expr_id).await?;
                exprs_to_trans_tasks
                    .entry(repr_logical_expr_id)
                    .or_insert_with(HashMap::new)
                    .entry(task.rule.clone())
                    .or_insert_with(Vec::new)
                    .push((task_id, logical_expr_id));
            }

            for expr in all_exprs.iter() {
                if !exprs_to_trans_tasks.contains_key(expr) {
                    // If there is not task for this expr, then we need to create a new one.
                    let transformations = self.rule_book.get_transformations().to_vec();
                    self.create_transformation_tasks(*expr, *repr_group_task_id, transformations)
                        .await?;
                } else {
                    let all_tasks_for_this_expr = exprs_to_trans_tasks.get(expr).unwrap().clone();
                    for rule in self.rule_book.get_transformations().to_vec() {
                        if !all_tasks_for_this_expr.contains_key(&rule) {
                            self.create_transform_expression_task(
                                rule,
                                *expr,
                                *repr_group_task_id,
                            )
                            .await?;
                        } else if all_tasks_for_this_expr.get(&rule).unwrap().len() == 1 {
                            let (task_id, task_expr_id) =
                                all_tasks_for_this_expr.get(&rule).unwrap()[0];
                            if task_expr_id == *expr {
                                // The task is already the repr task, so we don't need to do anything.
                                continue;
                            } else {
                                // The task is not the repr task, so we need to update the task to point to the new expr.
                                let task = self
                                    .tasks
                                    .get_mut(task_id)
                                    .unwrap()
                                    .as_transform_expression_mut();
                                task.logical_expr_id = *expr;
                                task.explore_group_out = *repr_group_task_id;
                            }
                        } else {
                            // If there is more than one task for this expr, then we need to merge them.
                            for (task_id, task_expr_id) in
                                all_tasks_for_this_expr.get(&rule).unwrap()
                            {
                                if task_expr_id == expr {
                                    // The task is already the repr task, so we just make sure that it points to the new repr group.
                                    let task = self
                                        .tasks
                                        .get_mut(task_id)
                                        .unwrap()
                                        .as_transform_expression_mut();
                                    task.explore_group_out = *repr_group_task_id;
                                    continue;
                                } else {
                                    // The task is not the repr task, so we need to delete it.
                                    self.try_delete_task(**task_id);
                                }
                            }
                        }
                    }
                }
            }
        }

        for goal_merge in result.goal_merges {
            // let repr_goal_id = goal_merge.new_repr_goal_id;
            // let non_repr_goal_id = goal_merge.non_repr_goal_id;

            // let repr_goal_task_id = self.goal_optimization_task_index.get(&repr_goal_id);
            // let non_repr_goal_task_id = self.goal_optimization_task_index.get(&non_repr_goal_id);
            // if repr_goal_task_id.is_none() && non_repr_goal_task_id.is_none() {
            //     // There is no task for the repr goal or the non-repr goal, so we don't need to do anything.
            //     continue;
            // }
            // if repr_goal_task_id.is_none() {
            //     // TODO(Sarvesh): we need to create a new optimize goal task for the repr goal.
            //     self.create_optimize_goal_task(repr_goal_id, None).await?;
            // }
            // let repr_goal_task_id = &self
            //     .goal_optimization_task_index
            //     .get(&repr_goal_id)
            //     .unwrap()
            //     .clone();

            // if !goal_merge.repr_goal_seen_best_expr_before_merge && goal_merge.best_expr.is_some() {
            //     let (fork_costed_outs, optimize_goal_outs, optimize_plan_outs) = {
            //         let repr_goal_task = self
            //             .tasks
            //             .get_mut(repr_goal_task_id)
            //             .unwrap()
            //             .as_optimize_goal_mut();
            //         (
            //             repr_goal_task.fork_costed_out.clone(),
            //             repr_goal_task.optimize_goal_out.clone(),
            //             repr_goal_task.optimize_plan_out.clone(),
            //         )
            //     };

            //     for task_id in fork_costed_outs.iter() {
            //         let task = self.tasks.get_mut(task_id).unwrap().as_fork_costed_mut();
            //         task.optimize_goal_in = *repr_goal_task_id;
            //         self.create_continue_with_costed_task(
            //             goal_merge.best_expr.unwrap().0.clone(),
            //             goal_merge.best_expr.unwrap().1.clone(),
            //             *task_id,
            //         )
            //         .await?;
            //     }

            //     for task_id in optimize_plan_outs.iter() {
            //         let task = self.tasks.get_mut(task_id).unwrap().as_optimize_plan_mut();
            //         task.optimize_goal_in = *repr_goal_task_id;
            //         let tx = task.physical_plan_tx.clone();
            //         self.emit_best_physical_plan(tx, goal_merge.best_expr.unwrap().0.clone())
            //             .await?;
            //     }

            //     for task_id in optimize_goal_outs.iter() {
            //         let task = self.tasks.get_mut(task_id).unwrap().as_optimize_goal_mut();
            //         let non_repr_goal_task_id = self
            //             .goal_optimization_task_index
            //             .get(&non_repr_goal_id)
            //             .unwrap()
            //             .clone();
            //         let mut to_be_inserted = true;
            //         for goal_id in task.optimize_goal_in.iter_mut() {
            //             if goal_id == &non_repr_goal_task_id {
            //                 *goal_id = *repr_goal_task_id;
            //                 to_be_inserted = false;
            //             }
            //         }
            //         if to_be_inserted {
            //             task.optimize_goal_in.push(*repr_goal_task_id);
            //         }
            //     }
            // }

            // let forward_result = self
            //     .memo
            //     .add_goal_member(
            //         goal_merge.new_repr_goal_id,
            //         GoalMemberId::GoalId(goal_merge.non_repr_goal_id),
            //     )
            //     .await?;
            // if let Some(forward_result) = forward_result {
            //     self.handle_forward_result(forward_result).await?;
            // }

            // let forward_result = self
            //     .memo
            //     .add_goal_member(
            //         goal_merge.non_repr_goal_id,
            //         GoalMemberId::GoalId(goal_merge.new_repr_goal_id),
            //     )
            //     .await?;
            // if let Some(forward_result) = forward_result {
            //     self.handle_forward_result(forward_result).await?;
            // }
            let non_repr_goal_id = goal_merge.non_repr_goal_id;
            let repr_goal_id = goal_merge.new_repr_goal_id;
            let non_repr_goal_task_id =
                if let Some(id) = self.goal_optimization_task_index.get(&non_repr_goal_id) {
                    *id
                } else {
                    continue;
                };

            let repr_goal_task_id =
                if let Some(id) = self.goal_optimization_task_index.get(&repr_goal_id) {
                    *id
                } else {
                    continue;
                };
            // Get the data we need from non_repr_goal_task first
            let non_repr_goal_out;
            let non_repr_plan_out;
            let non_repr_fork_costed_out;
            {
                let non_repr_goal_task = self
                    .tasks
                    .get_mut(&non_repr_goal_task_id)
                    .unwrap()
                    .as_optimize_goal_mut();

                // TODO(Sarvesh) i want to move the outs and replace them with empty vectors without cloning
                // but mem::take looks ugly, so i'm just going to clone for now
                non_repr_goal_out = non_repr_goal_task.optimize_goal_out.clone();
                non_repr_goal_task.optimize_goal_out = Vec::new();
                non_repr_plan_out = non_repr_goal_task.optimize_plan_out.clone();
                non_repr_goal_task.optimize_plan_out = Vec::new();
                non_repr_fork_costed_out = non_repr_goal_task.fork_costed_out.clone();
                non_repr_goal_task.fork_costed_out = Vec::new();
            }

            // Now update repr_goal_task with the collected data
            let repr_goal_task = self
                .tasks
                .get_mut(&repr_goal_task_id)
                .unwrap()
                .as_optimize_goal_mut();
            repr_goal_task.optimize_goal_out.extend(non_repr_goal_out);
            repr_goal_task.optimize_plan_out.extend(non_repr_plan_out);
            repr_goal_task
                .fork_costed_out
                .extend(non_repr_fork_costed_out);

            // delete the non-repr goal task
            self.try_delete_task(non_repr_goal_task_id);
        }

        for expr_merge in result.physical_expr_merges {
            let repr_expr_id = expr_merge.repr_physical_expr;
            let non_repr_expr_id = expr_merge.non_repr_physical_exprs;

            if let Some(repr_task_id) = self.cost_expression_task_index.get(&repr_expr_id) {
                // delete all the non-repr ones
                if let Some(non_repr_task_id) =
                    self.cost_expression_task_index.get(&non_repr_expr_id)
                {
                    // cost expression
                    let task = self
                        .tasks
                        .get_mut(non_repr_task_id)
                        .unwrap()
                        .as_cost_expression_mut();

                    for goal_task_id in task.optimize_goal_out.clone().iter() {
                        if let Some(goal_task) = self.tasks.get_mut(goal_task_id) {
                            goal_task
                                .as_optimize_goal_mut()
                                .cost_expression_in
                                .remove(non_repr_task_id);
                            goal_task
                                .as_optimize_goal_mut()
                                .cost_expression_in
                                .insert(*repr_task_id);
                        }
                    }
                    self.try_delete_task(*non_repr_task_id);
                }
            } else if let Some(non_repr_task_id) =
                self.cost_expression_task_index.get(&non_repr_expr_id)
            {
                // update the task
                let task = self
                    .tasks
                    .get_mut(non_repr_task_id)
                    .unwrap()
                    .as_cost_expression_mut();
                task.physical_expr_id = repr_expr_id;
            }
        }
        // for group_merge in result.group_merges {
        //     // For each MergedGroupInfo
        //     // 1. For each group
        //     // - Get all new expressions in that group (see code currently in my branch, just take all, then remove the ones that are in the group).
        //     // - Launch new subtasks based on subscription:
        //     // (this only happens if there is a task for the group).
        //     // -> Implement expression, Explore expression, ForkLogical
        //     let repr_group_id = group_merge.new_repr_group_id;
        //     // TODO(Sarvesh): should we ensure that the explore group is running?

        //     for group_info in &group_merge.merged_groups {
        //         let merged_group_id = group_info.group_id;
        //         let merged_group_task_id = self.group_exploration_task_index.get(&merged_group_id);
        //         if merged_group_task_id.is_none() {
        //             // TODO(Sarvesh): we ignore this merged group, as it is not running and hence, has no subscribers
        //             continue;
        //         };
        //         explore_group_tasks.push((explore_group_task_id, *group_id));

        //         let (optimize_goal_task_ids, fork_logical_task_ids) = {
        //             let explore_group_task = self
        //                 .tasks
        //                 .get(&explore_group_task_id)
        //                 .unwrap()
        //                 .as_explore_group();

        //             (
        //                 explore_group_task.optimize_goal_out.clone(),
        //                 explore_group_task.fork_logical_out.clone(),
        //             )
        //         };

        //         let other_groups_exprs: Vec<_> = all_exprs_by_group
        //             .iter()
        //             .filter(|(id, _)| *id != group_id)
        //             .flat_map(|(_, exprs)| exprs)
        //             .cloned()
        //             .collect::<HashSet<LogicalExpressionId>>();

        //         for logical_expr_id in other_groups_exprs {
        //             let transformations = self.rule_book.get_transformations().to_vec();

        //             // Create transformations for each logical expression from the other groups
        //             let xform_expr_task_ids = self
        //                 .create_transformation_tasks(
        //                     logical_expr_id,
        //                     explore_group_task_id,
        //                     transformations,
        //                 )
        //                 .await?;

        //             let explore_group_task = self
        //                 .get_task_mut(explore_group_task_id)
        //                 .as_explore_group_mut();
        //             for task_id in xform_expr_task_ids {
        //                 explore_group_task.add_transform_expr_in(task_id);
        //             }

        //             // Create implementations for each logical expression from the other groups
        //             // for each goal that depends on the group.
        //             for optimize_goal_task_id in optimize_goal_task_ids.iter() {
        //                 let goal_id = {
        //                     let optimize_goal_task = self
        //                         .tasks
        //                         .get(optimize_goal_task_id)
        //                         .unwrap()
        //                         .as_optimize_goal();
        //                     optimize_goal_task.goal_id
        //                 };

        //                 let implementations = self.rule_book.get_implementations().to_vec();

        //                 let impl_expr_task_ids = self
        //                     .create_implementation_tasks(
        //                         logical_expr_id,
        //                         goal_id,
        //                         *optimize_goal_task_id,
        //                         implementations,
        //                     )
        //                     .await?;

        //                 let optimize_goal_task = self
        //                     .tasks
        //                     .get_mut(optimize_goal_task_id)
        //                     .unwrap()
        //                     .as_optimize_goal_mut();

        //                 for task_id in impl_expr_task_ids {
        //                     optimize_goal_task.add_implement_expr_in(task_id);
        //                 }
        //             }

        //             // Create continuation for each logical expression from the other groups
        //             // at each fork point that depends on the group.
        //             for fork_logical_task_id in fork_logical_task_ids.iter() {
        //                 let cont_with_logical_task_id = self
        //                     .create_continue_with_logical_task(
        //                         logical_expr_id,
        //                         *fork_logical_task_id,
        //                     )
        //                     .await?;
        //                 let fork_logical_task = self
        //                     .tasks
        //                     .get_mut(fork_logical_task_id)
        //                     .unwrap()
        //                     .as_fork_logical_mut();
        //                 fork_logical_task.add_continue_in(cont_with_logical_task_id);
        //             }
        //         }
        //     }
        //     // 2. Merge the entire graph as follows:
        //     // - Get all explore group tasks from each merged group from the index
        //     // -> If none, do nothing
        //     // -> Exactly one task present:
        //     // --> Update indexes
        //     // -> If more than one:
        //     // --> Take one, and merge all hashsets.
        //     // --> Update indexes

        //     let mut merged_optimize_goal_outs = HashSet::new();
        //     let mut merged_fork_logical_outs = HashSet::new();
        //     let mut merged_transform_expr_ins = HashSet::new();

        //     for group_info in &group_merge.merged_groups {
        //         let group_id = group_info.group_id;
        //         let task_id = self.group_exploration_task_index.get(&group_id);
        //         if task_id.is_none() {
        //             continue;
        //         }
        //         let task_id = task_id.unwrap();
        //         let task = self.tasks.get_mut(task_id).unwrap().as_explore_group_mut();
        //         merged_optimize_goal_outs.extend(task.optimize_goal_out.clone());
        //         merged_fork_logical_outs.extend(task.fork_logical_out.clone());
        //         merged_transform_expr_ins.extend(task.transform_expr_in.clone());

        //         if group_id == repr_group_id {
        //             continue;
        //         }

        //         self.tasks.remove(task_id);
        //         self.group_exploration_task_index.remove(&group_id);

        //         // TODO(Sarvesh): update all the ins and outs of the task
        //     }

        //     if merged_fork_logical_outs.is_empty() && merged_optimize_goal_outs.is_empty() {
        //         // There are no tasks that need this merged explore group to exist, so we can just remove it.
        //         // This case is only possible if a cascading merge has merged two groups that have no tasks that need their outputs.
        //         continue;
        //     }

        //     let repr_explore_task_id = self.group_exploration_task_index.get(&repr_group_id);
        //     let repr_explore_task_id = if let Some(repr_explore_task_id) = repr_explore_task_id {
        //         repr_explore_task_id
        //     } else {
        //         &self.create_explore_group_task(repr_group_id, None).await?.0
        //     };

        //     // remove duplicate transform expr tasks
        //     let mut publishers_to_remove = HashSet::new();
        //     for expr in merged_transform_expr_ins.iter() {
        //         let task = self.tasks.get(expr).unwrap();
        //         let transform_expr_task = task.as_transform_expression();
        //         let repr_logical_expr_id = transform_expr_task.logical_expr_id;
        //         if !group_merge
        //             .all_exprs
        //             .contains(&transform_expr_task.logical_expr_id)
        //         {
        //             publishers_to_remove.insert(*expr);
        //         }
        //     }

        //     for expr in publishers_to_remove {
        //         // TODO(Sarvesh): can we simply remove the task or do we need to ensure that it's children are also removed?
        //         // No, we cannot just do this. We must delete literrally all the tasks that publish to it and also delete the subtree.
        //         self.tasks.remove(&expr);
        //         merged_transform_expr_ins.remove(&expr);
        //     }

        //     let repr_explore_task = self
        //         .tasks
        //         .get_mut(repr_explore_task_id)
        //         .unwrap()
        //         .as_explore_group_mut();

        //     // we then update all the metadata of the explore group task
        //     repr_explore_task
        //         .optimize_goal_out
        //         .extend(merged_optimize_goal_outs);
        //     repr_explore_task
        //         .fork_logical_out
        //         .extend(merged_fork_logical_outs);
        //     repr_explore_task
        //         .transform_expr_in
        //         .extend(merged_transform_expr_ins);

        //     // 3. Go to all subscribers of that consolidated task, and filter
        //     // out all the tasks that have the same logical expression id (using the repr).
        //     // --> Cancel those tasks (i.e. remove from HashMap) and remove from graph.
        //     // TODO(Sarvesh): implement this: there can be multiple fork outs that
        // }

        // // ## Apply goal merges:
        // // @alexis
        // // 1. For each goal
        // // - Get all new members in that goal (ditto).
        // // - Get optional new best cost from all (also see code currently on my branch!).
        // // - For all subscribers to goal notify the best new cost.
        // // -> Continuations, OptimizePlan [OptimizeGoal is a bit different, as they also
        // // need the new members to do anything].
        // // [Aside] for OptimizeGoal: send the new members and launch tasks accordingly
        // // (i.e. CostExpression, and OptimizeGoal).

        // for goal_merge in result.goal_merges {
        //     // 1. For each goal, schedule the best expression from all OTHER goals only if it is
        //     // better than the current best expression for the goal.
        //     if let Some(best_expr) = goal_merge.best_expr {
        //         for (i, current_goal_info) in goal_merge.merged_goals.iter().enumerate() {
        //             if current_goal_info.seen_best_expr_before_merge {
        //                 continue;
        //             }

        //             // Schedule the best expression
        //             let merge_goal_task = {
        //                 let merged_goal_task_id = self
        //                     .goal_optimization_task_index
        //                     .get(&current_goal_info.goal_id);
        //                 if merged_goal_task_id.is_none() {
        //                     continue;
        //                 } else {
        //                     let merged_goal_task_id = merged_goal_task_id.unwrap();
        //                     self.tasks
        //                         .get_mut(merged_goal_task_id)
        //                         .unwrap()
        //                         .as_optimize_goal_mut()
        //                 }
        //             };

        //             let optimize_plan_outs = merge_goal_task.optimize_plan_out.clone();
        //             let fork_costed_outs = merge_goal_task.fork_costed_out.clone();

        //             // Schedule out tasks
        //             for task_id in optimize_plan_outs.iter() {
        //                 let task = self.tasks.get(task_id).unwrap().as_optimize_plan();
        //                 self.emit_best_physical_plan(task.physical_plan_tx.clone(), best_expr.0)
        //                     .await?;
        //             }

        //             // For each dependent `ForkCosted` task, create a new `ContinueWithCosted` tasks for
        //             // the new best costed physical expression.

        //             for task_id in fork_costed_outs.iter() {
        //                 let continue_with_costed_task_id = self
        //                     .create_continue_with_costed_task(best_expr.0, best_expr.1, *task_id)
        //                     .await?;
        //                 let fork_costed_task =
        //                     self.tasks.get_mut(&task_id).unwrap().as_fork_costed_mut();
        //                 fork_costed_task.budget = best_expr.1;
        //                 fork_costed_task.add_continue_in(continue_with_costed_task_id);
        //             }
        //         }
        //     }

        //     for goal_info in &goal_merge.merged_goals {
        //         let goal_id = goal_info.goal_id;
        //         let task_id = self.goal_optimization_task_index.get(&goal_id);
        //         if task_id.is_none() {
        //             continue;
        //         }
        //     }
        // }

        // // 2. Merge the entire graph just like for groups.
        // // -> Except we also need to report the PhysicalExpressionID merged, so that we
        // // can adapt that index correctly too (not required for LogicalExpressionID).
        // // 3. Go to all subscribers of consolidated OptimizeGoal task, and remove the ones 	// appear twice using repr.

        // // ## Handle dirty stuff:
        // // Note(Sarvesh): we need this because the fork tasks may not exist
        // // 0. Aside: we need a way to know inside what group the dirty transformation/implementation belongs, as we don't have an index for that.
        // // 1. Find the task corresponding to the dirty cost expression / implement expression / transform expression.
        // // 2. Execute:
        // // - If does not exist, do nothing.
        // // - If exists, but has already started (boolean), do nothing.
        // // - If exists, and has not started: schedule the job, and set the flag to true.

        // // TODO: could we reuse `handle_forward_result` logic?
        // let members_from_others = all_exprs_by_goal
        //     .iter()
        //     .filter(|(id, _)| *id != goal_id)
        //     .flat_map(|(_, goal_info)| goal_info.members.iter())
        //     .cloned()
        //     .collect::<HashSet<_>>();

        // // Ensure there is a subtask for all the goal members from other goals.
        // for member in members_from_others {
        //     match member {
        //         GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
        //             let task_id = self
        //                 .ensure_cost_expression_task(
        //                     physical_expr_id,
        //                     Cost(f64::MAX),
        //                     optimize_goal_task_id,
        //                 )
        //                 .await?;
        //             let optimize_goal_task = self
        //                 .tasks
        //                 .get_mut(&optimize_goal_task_id)
        //                 .unwrap()
        //                 .as_optimize_goal_mut();
        //             optimize_goal_task.add_cost_expr_in(task_id);
        //         }
        //         GoalMemberId::GoalId(goal_id) => {
        //             let (task_id, _) = self
        //                 .ensure_optimize_goal_task(
        //                     goal_id,
        //                     SourceTaskId::OptimizeGoal(optimize_goal_task_id),
        //                 )
        //                 .await?;
        //             let optimize_goal_task = self
        //                 .tasks
        //                 .get_mut(&optimize_goal_task_id)
        //                 .unwrap()
        //                 .as_optimize_goal_mut();
        //             optimize_goal_task.add_optimize_goal_in(task_id);
        //         }
        //     }
        // }

        // let best_from_others = all_exprs_by_goal
        //     .iter()
        //     .filter(|(id, _)| *id != goal_id)
        //     .filter_map(|(_, goal_info)| goal_info.best_expr)
        //     .filter(|(_, cost)| current_cost.is_none_or(|current| *cost < current))
        //     .fold(None, |acc, (expr_id, cost)| match acc {
        //         None => Some((expr_id, cost)),
        //         Some((_, acc_cost)) if cost < acc_cost => Some((expr_id, cost)),
        //         Some(_) => acc,
        //     });

        // if let Some((best_expr_id, best_cost)) = best_from_others {
        //     // Send to client of optimize plan.
        //     for task_id in optimize_plan_task_ids.iter() {
        //         let optimize_plan_task = self.tasks.get(task_id).unwrap().as_optimize_plan();
        //         self.emit_best_physical_plan(
        //             optimize_plan_task.physical_plan_tx.clone(),
        //             best_expr_id,
        //         )
        //         .await?;
        //     }
        //     // Lanuch continuations.
        //     for task_id in fork_costed_task_ids.iter() {
        //         let continue_with_costed_task_id = self
        //             .create_continue_with_costed_task(best_expr_id, best_cost, *task_id)
        //             .await?;
        //         let fork_costed_task = self.tasks.get_mut(task_id).unwrap().as_fork_costed_mut();
        //         fork_costed_task.budget = best_cost;
        //         fork_costed_task.add_continue_in(continue_with_costed_task_id);
        //     }
        // }
        todo!()
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

    // Helper method to merge optimization tasks for merged goals.
    // async fn merge_optimization_tasks(
    //     &mut self,
    //     _all_exprs_by_goal: &[MergedGoalInfo],
    //     _new_repr_goal_id: GoalId,
    // ) {
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
    // }
}
