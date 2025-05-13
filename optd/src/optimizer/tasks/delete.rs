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
    pub(crate) fn delete_task(&mut self, task_id: TaskId) {
        use Task::*;

        let task = self.get_task(task_id).cloned().unwrap();
        match &task {
            TransformExpression(task) => {
                let explore_task = self
                    .get_explore_group_task_mut(task.explore_group_out)
                    .unwrap();
                explore_task.transform_expr_in.remove(&task_id);

                if let Some(fork_id) = task.fork_in {
                    self.delete_task(fork_id);
                }
            }

            ContinueWithLogical(task) => {
                let fork_task = self.get_fork_logical_task_mut(task.fork_out).unwrap();
                fork_task.continue_with_logical_in.remove(&task_id);

                if let Some(fork_id) = task.fork_in {
                    self.delete_task(fork_id);
                }
            }

            ForkLogical(task) => {
                let explore_task = self
                    .get_explore_group_task_mut(task.explore_group_in)
                    .unwrap();
                explore_task.fork_logical_out.remove(&task_id);
                // Delete explore task if it has no more purpose.
                if explore_task.fork_logical_out.is_empty()
                    && explore_task.optimize_goal_out.is_empty()
                {
                    self.delete_task(task.explore_group_in);
                }

                let continue_tasks: Vec<_> =
                    task.continue_with_logical_in.iter().copied().collect();
                for continue_id in continue_tasks {
                    self.delete_task(continue_id);
                }
            }

            ExploreGroup(task) => {
                assert!(task.fork_logical_out.is_empty());
                assert!(task.optimize_goal_out.is_empty());

                self.group_exploration_task_index
                    .retain(|_, &mut v| v != task_id);

                let transform_tasks: Vec<_> = task.transform_expr_in.iter().copied().collect();
                for transform_id in transform_tasks {
                    self.delete_task(transform_id);
                }
            }

            _ => {
                todo!();
            }
        }

        // Finally, remove the task from the task collection.
        self.remove_task(task_id);
    }
}
