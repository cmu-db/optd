use crate::{
    core::bridge::from_cir::costed_physical_to_value,
    core::cir::{Cost, PhysicalExpressionId},
    core::error::Error,
    core::optimizer::{JobId, Optimizer},
    memo::Memoize,
};

use super::{Task, TaskId};

#[derive(Debug)]
pub struct ContinueWithCostedTask {
    pub physical_expr_id: PhysicalExpressionId,
    pub cost: Cost,
    pub fork_out: TaskId,
    pub fork_in: Option<TaskId>,
}

impl ContinueWithCostedTask {
    pub fn new(physical_expr_id: PhysicalExpressionId, cost: Cost, fork_out: TaskId) -> Self {
        Self {
            physical_expr_id,
            fork_out,
            fork_in: None,
            cost,
        }
    }

    pub fn add_fork_in(&mut self, task_id: TaskId) {
        self.fork_in = Some(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    pub async fn create_continue_with_costed_task(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        cost: Cost,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();
        let task = ContinueWithCostedTask::new(physical_expr_id, cost, out);

        self.tasks.insert(task_id, Task::ContinueWithCosted(task));
        self.schedule_task_job(task_id);
        Ok(task_id)
    }

    /// Executes a job to continue processing with an optimized physical expression result.
    ///
    /// This materializes the physical expression and passes it along with its cost
    /// to the continuation.
    pub async fn execute_continue_with_costed(
        &self,
        task: &ContinueWithCostedTask,
        job_id: JobId,
    ) -> Result<(), Error> {
        let fork_task = self.tasks.get(&task.fork_out).unwrap().as_fork_costed();
        let k = fork_task.continuation.clone();
        let plan = self.egest_partial_plan(task.physical_expr_id).await?;
        let costed_plan_value = costed_physical_to_value(plan, task.cost);
        let engine_tx = self.engine_tx.clone();

        tokio::spawn(async move {
            let response = k(costed_plan_value).await;

            Self::send_engine_response(job_id, engine_tx, response).await;
        });

        Ok(())
    }
}
