#![allow(dead_code)]

use std::collections::HashSet;

use super::Optimizer;
use crate::{
    cir::{GoalId, GroupId, ImplementationRule, LogicalExpressionId},
    error::Error,
    memo::{Memoize, MergeResult, MergedGoalInfo, MergedGroupInfo},
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
            // For each MergedGroupInfo
            // 1. For each group
            // - Get all new expressions in that group (see code currently in my branch, just take all, then remove the ones that are in the group).
            // - Launch new subtasks based on subscription:
            // (this only happens if there is a task for the group).
            // -> Implement expression, Explore expression, ForkLogical
            let repr_group_id = group_merge.new_repr_group_id;
            // TODO(Sarvesh): should we ensure that the explore group is running?
            let repr_explore_task_id = self.group_exploration_task_index.get(&repr_group_id);
            if repr_explore_task_id.is_none() {
                // TODO(Sarvesh): I believe we should just ignore this merge
                continue;
            }

            for group_info in &group_merge.merged_groups {
                let merged_group_id = group_info.group_id;
                let merged_group_task_id = self.group_exploration_task_index.get(&merged_group_id);
                if merged_group_task_id.is_none() {
                    // TODO(Sarvesh): we ignore this merged group, as it is not running and hence, has no subscribers
                    continue;
                }
                let merged_group_task_id = merged_group_task_id.unwrap();
                let merged_group_task = self
                    .tasks
                    .get_mut(&merged_group_task_id)
                    .unwrap()
                    .as_explore_group();

                let fork_outs = merged_group_task.fork_logical_out.clone();
                let optimize_goal_outs = merged_group_task.optimize_goal_out.clone();

                let old_exprs = HashSet::from_iter(group_info.expressions.iter().copied());

                let new_exprs = group_merge
                    .all_exprs
                    .iter()
                    .filter(|expr| !old_exprs.contains(expr));

                for expr in new_exprs {
                    for fork_out in fork_outs.iter() {
                        // Create the continuation task
                        let cont_with_logical_task_id = {
                            self.create_continue_with_logical_task(*expr, *fork_out)
                                .await?
                        };

                        // Get the fork task first
                        let fork_task = self.tasks.get_mut(fork_out).unwrap().as_fork_logical_mut();
                        // Update the fork task
                        fork_task.add_continue_in(cont_with_logical_task_id);
                    }

                    for optimize_goal_out in optimize_goal_outs.iter() {
                        let implementations = self.rule_book.get_implementations().to_vec();
                        let optimize_goal_task = self
                            .tasks
                            .get_mut(optimize_goal_out)
                            .unwrap()
                            .as_optimize_goal_mut();
                        let goal_id = optimize_goal_task.goal_id;

                        let task_ids = self
                            .create_implementation_tasks(
                                *expr,
                                goal_id,
                                *optimize_goal_out,
                                &implementations,
                            )
                            .await?;

                        let optimize_goal_task = self
                            .tasks
                            .get_mut(optimize_goal_out)
                            .unwrap()
                            .as_optimize_goal_mut();
                        for task_id in task_ids {
                            optimize_goal_task.add_implement_expr_in(task_id);
                        }
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
        }

        // 3. Go to all subscribers and publishers of that consolidated task, and filter
        // out all the tasks that have the same logical expression id (using the repr).
        // --> Cancel those tasks (i.e. remove from HashMap) and remove from graph.

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
        // 2. Merge the entire graph just like for groups.
        // -> Except we also need to report the PhysicalExpressionID merged, so that we
        // can adapt that index correctly too (not required for LogicalExpressionID).
        // 3. Go to all subscribers of consolidated OptimizeGoal task, and remove the ones 	// appear twice using repr.

        // ## Handle dirty stuff:
        // 0. Aside: we need a way to know inside what group the dirty transformation/implementation belongs, as we don't have an index for that.
        // 1. Find the task corresponding to the dirty cost expression / implement expression / transform expression.
        // 2. Execute:
        // - If does not exist, do nothing.
        // - If exists, but has already started (boolean), do nothing.
        // - If exists, and has not started: schedule the job, and set the flag to true.

        // ## Apply goal merges:
        // Similar approach...
        // (@yuchen draft):
        // 1. For each goal
        // - Get the (old) best costed expression in that goal
        // - What to do with goal members?
        // - Launch new tasks blindly based on subscription:
        // -> CostExpression, ForkCostedPhysical
        // 2. Merge the entire graph as follows:
        // - Get all tasks from each merged goal
        // If none, do nothing.
        // Exactly one task present:
        // --> Update indexes
        // If more than one:
        // --> Take one and merge all hashsets.
        // --> Update indexes
        // 3. TODO:

        // Handle dirty transformation / implementation / costing.

        Ok(())
    }

    /// Helper method to merge exploration tasks for merged groups.
    ///
    /// # Parameters
    /// * `all_exprs_by_group` - All groups and their expressions that were merged.
    /// * `new_repr_group_id` - The new representative group ID.
    async fn merge_exploration_tasks(
        &mut self,
        _all_exprs_by_group: &[MergedGroupInfo],
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

    async fn create_implementation_tasks(
        &mut self,
        expr: LogicalExpressionId,
        goal_id: GoalId,
        optimize_goal_out: TaskId,
        implementations: &[ImplementationRule],
    ) -> Result<Vec<TaskId>, Error> {
        let mut task_ids = Vec::new();
        for rule in implementations {
            let impl_expr_task_id = self
                .create_implement_expression_task(rule.clone(), expr, goal_id, optimize_goal_out)
                .await?;
            task_ids.push(impl_expr_task_id);
        }
        Ok(task_ids)
    }
}
