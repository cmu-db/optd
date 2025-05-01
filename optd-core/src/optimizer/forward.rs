use crate::{
    error::Error,
    memo::{PropagateCost, Memoize},
};

use super::Optimizer;

impl<M: Memoize> Optimizer<M> {
    pub(super) async fn handle_forward_result(
        &mut self,
        result: PropagateCost,
    ) -> Result<(), Error> {
        for goal_id in result.goals_forwarded {
            // Only forward to goals if a task exists for it.
            if let Some(task_id) = self.goal_optimization_task_index.get(&goal_id) {
                // Emit the best physical plan to all dependent `OptimizePlan` tasks.
                let (optimize_plan_outs, fork_costed_outs) = {
                    let task = self.tasks.get_mut(task_id).unwrap().as_optimize_goal_mut();
                    (task.optimize_plan_out.clone(), task.fork_costed_out.clone())
                };

                for task_id in optimize_plan_outs.iter() {
                    let task = self.tasks.get(task_id).unwrap().as_optimize_plan();
                    self.emit_best_physical_plan(
                        task.physical_plan_tx.clone(),
                        result.physical_expr_id,
                    )
                    .await?;
                }

                // For each dependent `ForkCosted` task, create a new `ContinueWithCosted` tasks for
                // the new best costed physical expression.

                for task_id in fork_costed_outs {
                    let continue_with_costed_task_id = self
                        .create_continue_with_costed_task(
                            result.physical_expr_id,
                            result.best_cost,
                            task_id,
                        )
                        .await?;
                    let fork_costed_task =
                        self.tasks.get_mut(&task_id).unwrap().as_fork_costed_mut();
                    fork_costed_task.budget = result.best_cost;
                    fork_costed_task.add_continue_in(continue_with_costed_task_id);
                }
            }
        }

        Ok(())
    }
}
