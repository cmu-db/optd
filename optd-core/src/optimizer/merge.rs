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
            // For each MergedGroupInfo
            // 1. For each group
            // - Get all new expressions in that group (see code currently in my branch, just take all, then remove the ones that are in the group).
            // - Launch new subtasks based on subscription:
            // (this only happens if there is a task for the group).
            // -> Implement expression, Explore expression, ForkLogical
            let repr_group_id = group_merge.new_repr_group_id;
            // TODO(Sarvesh): should we ensure that the explore group is running?

            for group_info in &group_merge.merged_groups {
                let merged_group_id = group_info.group_id;
                let merged_group_task_id = self.group_exploration_task_index.get(&merged_group_id);
                if merged_group_task_id.is_none() {
                    // TODO(Sarvesh): we ignore this merged group, as it is not running and hence, has no subscribers
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
                    .collect::<HashSet<LogicalExpressionId>>();

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
            // 2. Merge the entire graph as follows:
            // - Get all explore group tasks from each merged group from the index
            // -> If none, do nothing
            // -> Exactly one task present:
            // --> Update indexes
            // -> If more than one:
            // --> Take one, and merge all hashsets.
            // --> Update indexes

            let mut merged_optimize_goal_outs = HashSet::new();
            let mut merged_fork_logical_outs = HashSet::new();
            let mut merged_transform_expr_ins = HashSet::new();

            for group_info in &group_merge.merged_groups {
                let group_id = group_info.group_id;
                let task_id = self.group_exploration_task_index.get(&group_id);
                if task_id.is_none() {
                    continue;
                }
                let task_id = task_id.unwrap();
                let task = self.tasks.get_mut(task_id).unwrap().as_explore_group_mut();
                merged_optimize_goal_outs.extend(task.optimize_goal_out.clone());
                merged_fork_logical_outs.extend(task.fork_logical_out.clone());
                merged_transform_expr_ins.extend(task.transform_expr_in.clone());

                if group_id == repr_group_id {
                    continue;
                }

                self.tasks.remove(task_id);
                self.group_exploration_task_index.remove(&group_id);

                // TODO(Sarvesh): update all the ins and outs of the task
            }

            if merged_fork_logical_outs.is_empty() && merged_optimize_goal_outs.is_empty() {
                // There are no tasks that need this merged explore group to exist, so we can just remove it.
                // This case is only possible if a cascading merge has merged two groups that have no tasks that need their outputs.
                continue;
            }

            let repr_explore_task_id = self.group_exploration_task_index.get(&repr_group_id);
            let repr_explore_task_id = if let Some(repr_explore_task_id) = repr_explore_task_id {
                repr_explore_task_id
            } else {
                &self.create_explore_group_task(repr_group_id, None).await?.0
            };

            // remove duplicate transform expr tasks
            let mut publishers_to_remove = HashSet::new();
            for expr in merged_transform_expr_ins.iter() {
                let task = self.tasks.get(expr).unwrap();
                let transform_expr_task = task.as_transform_expression();
                let repr_logical_expr_id = transform_expr_task.logical_expr_id;
                if !group_merge
                    .all_exprs
                    .contains(&transform_expr_task.logical_expr_id)
                {
                    publishers_to_remove.insert(*expr);
                }
            }

            for expr in publishers_to_remove {
                // TODO(Sarvesh): can we simply remove the task or do we need to ensure that it's children are also removed?
                // No, we cannot just do this. We must delete literrally all the tasks that publish to it and also delete the subtree.
                self.tasks.remove(&expr);
                merged_transform_expr_ins.remove(&expr);
            }

            let repr_explore_task = self
                .tasks
                .get_mut(repr_explore_task_id)
                .unwrap()
                .as_explore_group_mut();

            // we then update all the metadata of the explore group task
            repr_explore_task
                .optimize_goal_out
                .extend(merged_optimize_goal_outs);
            repr_explore_task
                .fork_logical_out
                .extend(merged_fork_logical_outs);
            repr_explore_task
                .transform_expr_in
                .extend(merged_transform_expr_ins);

            // 3. Go to all subscribers of that consolidated task, and filter
            // out all the tasks that have the same logical expression id (using the repr).
            // --> Cancel those tasks (i.e. remove from HashMap) and remove from graph.
            // TODO(Sarvesh): implement this: there can be multiple fork outs that 



        }

        // ## Apply goal merges:
        // @alexis
        // 1. For each goal
        // - Get all new members in that goal (ditto).
        // - Get optional new best cost from all (also see code currently on my branch!).
        // - For all subscribers to goal notify the best new cost.
        // -> Continuations, OptimizePlan [OptimizeGoal is a bit different, as they also
        // need the new members to do anything].
        // [Aside] for OptimizeGoal: send the new members and launch tasks accordingly
        // (i.e. CostExpression, and OptimizeGoal).

        for goal_merge in result.goal_merges {
            // 1. For each goal, schedule the best expression from all OTHER goals only if it is
            // better than the current best expression for the goal.
            if let Some(best_expr) = goal_merge.best_expr {
                for (i, current_goal_info) in goal_merge.merged_goals.iter().enumerate() {
                    if current_goal_info.seen_best_expr_before_merge {
                        continue;
                    }

                    // Schedule the best expression
                    let merge_goal_task = {
                        let merged_goal_task_id = self
                            .goal_optimization_task_index
                            .get(&current_goal_info.goal_id);
                        if merged_goal_task_id.is_none() {
                            continue;
                        } else {
                            let merged_goal_task_id = merged_goal_task_id.unwrap();
                            self.tasks
                                .get_mut(merged_goal_task_id)
                                .unwrap()
                                .as_optimize_goal_mut()
                        }
                    };

                    let optimize_plan_outs = merge_goal_task.optimize_plan_out.clone();
                    let fork_costed_outs = merge_goal_task.fork_costed_out.clone();

                    // Schedule out tasks
                    for task_id in optimize_plan_outs.iter() {
                        let task = self.tasks.get(task_id).unwrap().as_optimize_plan();
                        self.emit_best_physical_plan(task.physical_plan_tx.clone(), best_expr.0)
                            .await?;
                    }

                    // For each dependent `ForkCosted` task, create a new `ContinueWithCosted` tasks for
                    // the new best costed physical expression.

                    for task_id in fork_costed_outs.iter() {
                        let continue_with_costed_task_id = self
                            .create_continue_with_costed_task(best_expr.0, best_expr.1, *task_id)
                            .await?;
                        let fork_costed_task =
                            self.tasks.get_mut(&task_id).unwrap().as_fork_costed_mut();
                        fork_costed_task.budget = best_expr.1;
                        fork_costed_task.add_continue_in(continue_with_costed_task_id);
                    }
                }
            }

            for goal_info in &goal_merge.merged_goals {
                let goal_id = goal_info.goal_id;
                let task_id = self.goal_optimization_task_index.get(&goal_id);
                if task_id.is_none() {
                    continue;
                }
            }
        }

        // 2. Merge the entire graph just like for groups.
        // -> Except we also need to report the PhysicalExpressionID merged, so that we
        // can adapt that index correctly too (not required for LogicalExpressionID).
        // 3. Go to all subscribers of consolidated OptimizeGoal task, and remove the ones 	// appear twice using repr.

        // ## Handle dirty stuff:
        // Note(Sarvesh): we need this because the fork tasks may not exist
        // 0. Aside: we need a way to know inside what group the dirty transformation/implementation belongs, as we don't have an index for that.
        // 1. Find the task corresponding to the dirty cost expression / implement expression / transform expression.
        // 2. Execute:
        // - If does not exist, do nothing.
        // - If exists, but has already started (boolean), do nothing.
        // - If exists, and has not started: schedule the job, and set the flag to true.

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
