use crate::{
    bridge::from_cir::partial_logical_to_value,
    cir::LogicalExpressionId,
    error::Error,
    memo::Memoize,
    optimizer::{Job, JobId, Optimizer},
};

use super::{Task, TaskId};

#[derive(Debug)]
pub(super) struct ContinueWithLogicalTask {
    pub logical_expr_id: LogicalExpressionId,
    pub fork_out: TaskId,
    pub fork_in: Option<TaskId>,
}

impl ContinueWithLogicalTask {
    pub fn new(logical_expr_id: LogicalExpressionId, fork_out: TaskId) -> Self {
        Self {
            logical_expr_id,
            fork_out,
            fork_in: None,
        }
    }
}

impl<M: Memoize> Optimizer<M> {
    /// Creates a `ContinueWithLogical` task.
    pub async fn create_continue_with_logical_task(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();
        let task = ContinueWithLogicalTask::new(logical_expr_id, out);
        self.tasks.insert(task_id, Task::ContinueWithLogical(task));
        self.schedule_job(Job::Task(task_id));
        Ok(task_id)
    }

    /// Executes a job to continue processing with a logical expression result.
    ///
    /// This materializes the logical expression and passes it to the continuation.
    pub async fn execute_continue_with_logical(
        &self,
        task: &ContinueWithLogicalTask,
        job_id: JobId,
    ) -> Result<(), Error> {
        let fork_task = self.tasks.get(&task.fork_out).unwrap().as_fork_logical();
        let k = fork_task.continuation.clone();
        let plan = self
            .memo
            .materialize_logical_expr(task.logical_expr_id)
            .await?;
        let message_tx = self.message_tx.clone();

        tokio::spawn(async move {
            let response = k(partial_logical_to_value(&plan.into())).await;
            Self::send_engine_response(job_id, message_tx, response)
        });

        Ok(())
    }
}
