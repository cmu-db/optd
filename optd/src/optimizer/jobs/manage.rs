use super::{Job, JobId};
use crate::{
    memo::Memo,
    optimizer::{Optimizer, TaskId, errors::OptimizeError, jobs::JobKind},
};

impl<M: Memo> Optimizer<M> {
    /// Schedules a new job and associates it with a task.
    ///
    /// This method creates a job of the specified kind, associates it with
    /// the given task, adds it to the pending jobs collection, and updates
    /// the task's uncompleted jobs set.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task that's launching this job.
    /// * `kind` - The kind of job to create.
    ///
    /// # Returns
    /// * The ID of the created job.
    pub(crate) fn schedule_job(&mut self, task_id: TaskId, kind: JobKind) -> JobId {
        // Generate a new job ID.
        let job_id = self.next_job_id;
        self.next_job_id.0 += 1;

        // Create & schedule the job.
        let job = Job(task_id, kind);
        self.pending_jobs.insert(job_id, job);
        self.job_schedule_queue.push_back(job_id);

        // Add job to task's uncompleted jobs set.
        self.tasks
            .get_mut(&task_id)
            .unwrap()
            .uncompleted_jobs
            .insert(job_id);

        job_id
    }

    /// Launches all pending jobs until either the maximum concurrent job limit is
    /// reached or there are no more jobs to launch.
    ///
    /// Jobs are launched in LIFO order from the job schedule queue if the number
    /// of currently running jobs is below the maximum concurrent jobs limit.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during job launching.
    pub(crate) async fn launch_pending_jobs(&mut self) -> Result<(), OptimizeError> {
        use JobKind::*;

        // Launch jobs only if we're below the maximum concurrent jobs limit, in LIFO order.
        while self.running_jobs.len() < self.max_concurrent_jobs
            && !self.job_schedule_queue.is_empty()
        {
            let job_id = self.job_schedule_queue.pop_back().unwrap();

            // Move the job from pending to running.
            let job = self.pending_jobs.remove(&job_id).unwrap();
            self.running_jobs.insert(job_id, job.clone());

            // Dispatch & execute the job in a new co-routine.
            match job.1 {
                Derive(logical_expr_id) => {
                    self.derive_logical_properties(logical_expr_id, job_id)
                        .await?;
                }
                TransformExpression(rule_name, logical_expr_id, group_id) => {
                    self.execute_transformation_rule(rule_name, logical_expr_id, group_id, job_id)
                        .await?;
                }
                ImplementExpression(rule_name, expression_id, goal_id) => {
                    self.execute_implementation_rule(rule_name, expression_id, goal_id, job_id)
                        .await?;
                }
                CostExpression(expression_id) => {
                    self.execute_cost_expression(expression_id, job_id).await?;
                }
                ContinueWithLogical(expression_id, k) => {
                    self.execute_continue_with_logical(expression_id, k, job_id)
                        .await?;
                }
                ContinueWithCosted(expression_id, k) => {
                    self.execute_continue_with_costed(expression_id, k, job_id)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Marks a job as completed and updates related task status.
    ///
    /// This method removes the job from running jobs, updates the task's
    /// uncompleted jobs set, and marks the task as clean if it has no more
    /// uncompleted jobs.
    ///
    /// # Parameters
    /// * `job_id` - The ID of the job to mark as completed.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during job completion.
    pub(crate) async fn complete_job(&mut self, job_id: JobId) -> Result<(), OptimizeError> {
        // Remove the job from the running jobs.
        let Job(task_id, _) = self.running_jobs.remove(&job_id).unwrap();

        // Remove the job from the task's uncompleted jobs set.
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.uncompleted_jobs.remove(&job_id);

        Ok(())
    }

    /// Retrieves the task id associated with a specific job id.
    ///
    /// # Parameters
    /// * `job_id` - The ID of the job to find the related task for.
    ///
    /// # Returns
    /// * `TaskId` - The task id associated with the job id.
    pub(crate) fn get_related_task_id(&self, job_id: JobId) -> TaskId {
        let Job(task_id, _) = self.running_jobs.get(&job_id).unwrap();
        *task_id
    }
}
