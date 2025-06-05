use super::TaskId;
use crate::{
    memo::Memo,
    optimizer::{Optimizer, tasks::Task},
};

impl<M: Memo> Optimizer<M> {
    /// Helper method to recursively delete a task and clean up all references to it.
    ///
    /// This method acts as a garbage collector for the task graph, ensuring that
    /// when a task is deleted, all references to it are cleaned up and dependent
    /// tasks that no longer have a purpose are also deleted.
    ///
    /// # Parameters
    /// * `task_id` - ID of the task to delete.
    #[tracing::instrument(skip(self), fields(task_id = ?task_id), target = "optd::optimizer::tasks")]
    pub(crate) fn delete_task(&mut self, task_id: TaskId) {
        use Task::*;

        let task = self.get_task(task_id).cloned().unwrap();
        tracing::debug!(target: "optd::optimizer::tasks", task_type = %task.task_type(), "Starting task deletion");

        match &task {
            TransformExpression(task) => {
                tracing::trace!(target: "optd::optimizer::tasks", explore_group_out = ?task.explore_group_out, "Removing transform task from explore group");
                let explore_task = self
                    .get_explore_group_task_mut(task.explore_group_out)
                    .unwrap();
                explore_task.transform_expr_in.remove(&task_id);

                if let Some(fork_id) = task.fork_in {
                    tracing::trace!(target: "optd::optimizer::tasks", fork_id = ?fork_id, "Recursively deleting fork task");
                    self.delete_task(fork_id);
                }
            }
            ImplementExpression(implement_expression_task) => {
                tracing::trace!(target: "optd::optimizer::tasks", optimize_goal_out = ?implement_expression_task.optimize_goal_out, "Removing implement task from goal");
                let optimize_goal_task = self
                    .get_optimize_goal_task_mut(implement_expression_task.optimize_goal_out)
                    .unwrap();
                optimize_goal_task.implement_expression_in.remove(&task_id);

                if let Some(fork_id) = implement_expression_task.fork_in {
                    tracing::trace!(target: "optd::optimizer::tasks", fork_id = ?fork_id, "Recursively deleting fork task");
                    self.delete_task(fork_id);
                }
            }
            ContinueWithLogical(task) => {
                tracing::trace!(target: "optd::optimizer::tasks", fork_out = ?task.fork_out, "Removing continue task from fork");
                let fork_task = self.get_fork_logical_task_mut(task.fork_out).unwrap();
                fork_task.continue_with_logical_in.remove(&task_id);

                if let Some(fork_id) = task.fork_in {
                    tracing::trace!(target: "optd::optimizer::tasks", fork_id = ?fork_id, "Recursively deleting fork task");
                    self.delete_task(fork_id);
                }
            }
            ForkLogical(task) => {
                tracing::trace!(target: "optd::optimizer::tasks", explore_group_in = ?task.explore_group_in, "Removing fork task from explore group");
                let explore_task = self
                    .get_explore_group_task_mut(task.explore_group_in)
                    .unwrap();
                explore_task.fork_logical_out.remove(&task_id);

                // Delete explore task if it has no more purpose.
                if explore_task.fork_logical_out.is_empty()
                    && explore_task.optimize_goal_out.is_empty()
                {
                    tracing::debug!(target: "optd::optimizer::tasks", explore_task_id = ?task.explore_group_in, "Explore task has no more purpose, deleting");
                    self.delete_task(task.explore_group_in);
                }

                let continue_tasks: Vec<_> =
                    task.continue_with_logical_in.iter().copied().collect();
                tracing::trace!(target: "optd::optimizer::tasks", num_continue_tasks = continue_tasks.len(), "Recursively deleting continue tasks");
                for continue_id in continue_tasks {
                    self.delete_task(continue_id);
                }
            }
            ExploreGroup(task) => {
                assert!(task.fork_logical_out.is_empty());
                assert!(task.optimize_goal_out.is_empty());

                tracing::trace!(target: "optd::optimizer::tasks", "Removing explore group task from index");
                self.group_exploration_task_index
                    .retain(|_, &mut v| v != task_id);

                let transform_tasks: Vec<_> = task.transform_expr_in.iter().copied().collect();
                tracing::trace!(target: "optd::optimizer::tasks", num_transform_tasks = transform_tasks.len(), "Recursively deleting transform tasks");
                for transform_id in transform_tasks {
                    self.delete_task(transform_id);
                }
            }
            OptimizeGoal(task) => {
                assert!(task.optimize_goal_out.is_empty());
                assert!(task.optimize_plan_out.is_empty());

                tracing::trace!(target: "optd::optimizer::tasks", "Removing optimize goal task from index");
                self.goal_optimization_task_index
                    .retain(|_, &mut v| v != task_id);

                let implement_tasks: Vec<_> =
                    task.implement_expression_in.iter().copied().collect();
                tracing::trace!(target: "optd::optimizer::tasks", num_implement_tasks = implement_tasks.len(), "Recursively deleting implement tasks");
                for implement_id in implement_tasks {
                    self.delete_task(implement_id);
                }
            }
            OptimizePlan(_) => {
                tracing::warn!(target: "optd::optimizer::tasks", "OptimizePlan task deletion not yet implemented");
                todo!()
            },
        }

        // Finally, remove the task from the task collection.
        tracing::info!(target: "optd::optimizer::tasks", "Task deletion completed");
        self.remove_task(task_id);
    }
}
