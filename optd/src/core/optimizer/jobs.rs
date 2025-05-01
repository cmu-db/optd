use super::{EngineMessage, Task, TaskId};
use super::{EngineMessageKind, Optimizer};
use crate::core::bridge::from_cir::partial_logical_to_value;
use crate::core::bridge::into_cir::{
    hir_goal_to_cir, hir_group_id_to_cir, value_to_logical_properties,
};
use crate::core::cir::{
    GoalId, GroupId, ImplementationRule, LogicalExpressionId, LogicalProperties,
    PartialLogicalPlan, PhysicalExpressionId, TransformationRule,
};
use crate::core::error::Error;
use crate::core::memo::Memoize;
use crate::dsl::engine::{Engine, EngineResponse};
use EngineMessageKind::*;
use futures::SinkExt;
use futures::channel::mpsc;
use std::collections::hash_map::Entry;
use std::sync::Arc;

/// Unique identifier for jobs in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(pub i64);

#[derive(Debug)]
#[allow(unused)]
pub enum JobRoot {
    Transform(LogicalExpressionId, TransformationRule, GroupId),
    Implement(LogicalExpressionId, ImplementationRule, GoalId),
    Cost(PhysicalExpressionId),
}

/// A job represents a discrete unit of work within the optimization process.
///
/// Jobs are launched by tasks and represent atomic operations that contribute to
/// completing the task. Multiple jobs may be launched by a single task, and all
/// jobs must complete before a task is considered (temporarily) finished.
///
/// Each variant represents a specific optimization operation that can be
/// performed asynchronously and independently.
pub enum Job {
    /// Executes a job associated with a task.
    Task(TaskId),
    /// Derives logical properties for a logical expression.
    ///
    /// This job computes schema, cardinality estimates, and other
    /// statistical properties of a logical expression.
    Derive(GroupId),
}

impl<M: Memoize> Optimizer<M> {
    //
    // Job Scheduling and Management
    //

    /// Schedules a new job and associates it with a task.
    ///
    /// This method creates a job associates it with
    /// the given task, adds it to the runnable queue.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task that's launching this job.
    /// # Returns
    /// * The ID of the created job.
    pub(super) fn schedule_task_job(&mut self, task_id: TaskId) -> JobId {
        let job_id = self.next_job_id;
        self.next_job_id.0 += 1;
        self.runnable_jobs.insert(job_id, Job::Task(task_id));
        self.runnable_queue.push_back(job_id);
        job_id
    }

    pub(super) fn schedule_derive_job(
        &mut self,
        group_id: GroupId,
        sender: mpsc::Sender<LogicalProperties>,
    ) -> JobId {
        let entry = self.pending_derives.entry(group_id);
        match entry {
            Entry::Occupied(mut entry) => {
                let (job_id, senders) = entry.get_mut();
                senders.push(sender);
                *job_id
            }
            Entry::Vacant(entry) => {
                let job_id = self.next_job_id;
                self.next_job_id.0 += 1;
                self.running_jobs.insert(job_id, Job::Derive(group_id));
                // Push to the front of the queue to prioritize it,
                // and therefore the job that depends on it can make more progress sonner.
                self.runnable_queue.push_front(job_id);

                entry.insert((job_id, vec![sender]));
                job_id
            }
        }
    }

    /// Launches all runnable jobs until either the maximum concurrent job limit is
    /// reached or there are no more jobs to launch.
    ///
    /// Jobs are launched in FIFO order from the job schedule queue if the number
    /// of currently running jobs is below the maximum concurrent jobs limit.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during job launching.
    pub(super) async fn launch_runnable_jobs(&mut self) -> Result<(), Error> {
        // Launch jobs only if we're below the maximum concurrent jobs limit, in FIFO order.
        while self.running_jobs.len() < self.max_concurrent_jobs {
            let Some(job_id) = self.runnable_queue.pop_front() else {
                // No runnable jobs need to be launched.
                break;
            };

            let job = self.runnable_jobs.remove(&job_id).unwrap();

            match job {
                Job::Task(task_id) => {
                    // Move the job from pending to running.
                    let related_task = self.tasks.get(&task_id).unwrap();

                    // Dispatch & execute the job in a new co-routine.
                    match related_task {
                        Task::ImplementExpression(task) => {
                            self.execute_implementation_rule(task, job_id).await?;
                        }
                        Task::TransformExpression(task) => {
                            self.execute_transformation_rule(task, job_id).await?;
                        }
                        Task::CostExpression(task) => {
                            self.execute_cost_expression(task, job_id).await?;
                        }
                        Task::ContinueWithLogical(task) => {
                            self.execute_continue_with_logical(task, job_id).await?;
                        }
                        Task::ContinueWithCosted(task) => {
                            self.execute_continue_with_costed(task, job_id).await?;
                        }
                        task => {
                            panic!("{:?} should not have an associated job.", task);
                        }
                    }
                }
                Job::Derive(group_id) => {
                    self.derive_logical_properties(group_id, job_id).await?;
                }
            }
            self.running_jobs.insert(job_id, job);
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
    // TODO(yuchen): The engine should keep track of the root (expr_id, rule) pairs
    pub(super) async fn complete_job(&mut self, job_id: JobId) -> Result<(), Error> {
        // Remove the job from the running jobs.
        let _job = self.running_jobs.remove(&job_id).unwrap();

        // // Remove the job from the task's uncompleted jobs set.
        // let task = self.tasks.get_mut(&task_id).unwrap();
        // task.uncompleted_jobs.remove(&job_id);

        // // If the task has no uncompleted jobs, mark it as clean.
        // if task.uncompleted_jobs.is_empty() {
        //     match &task.kind {
        //         ImplementExpression(ImplementExpressionTask {
        //             rule,
        //             expression_id,
        //             goal_id,
        //             ..
        //         }) => {
        //             self.memo
        //                 .set_implementation_clean(*expression_id, *goal_id, rule)
        //                 .await?;
        //         }
        //         TransformExpression(TransformExpressionTask {
        //             expression_id,
        //             rule,
        //             ..
        //         }) => {
        //             self.memo
        //                 .set_transformation_clean(*expression_id, rule)
        //                 .await?;
        //         }
        //         CostExpression(task) => {
        //             self.memo.set_cost_clean(task.expr_id).await?;
        //         }
        //         _ => {} // We don't track status for the other task kinds.
        //     }
        // }

        // TODO(Alexis): Cleanup the parentless tasks to free up resources.

        Ok(())
    }

    pub(super) async fn send_engine_response(
        job_id: JobId,
        mut engine_tx: mpsc::Sender<EngineMessage>,
        response: EngineResponse<EngineMessageKind>,
    ) {
        match response {
            EngineResponse::Return(value, k) => {
                let msg = EngineMessage::new(job_id, k(value).await);
                engine_tx.send(msg).await.unwrap();
            }
            EngineResponse::YieldGroup(group_id, k) => {
                let msg =
                    EngineMessage::new(job_id, SubscribeGroup(hir_group_id_to_cir(&group_id), k));
                engine_tx.send(msg).await.unwrap();
            }
            EngineResponse::YieldGoal(goal, k) => {
                let msg = EngineMessage::new(job_id, SubscribeGoal(hir_goal_to_cir(&goal), k));
                engine_tx.send(msg).await.unwrap();
            }
        }
    }

    //
    // Job Execution Methods
    //

    /// Executes a job to derive logical properties for a logical expression.
    ///
    /// This creates an engine instance and launches the property derivation process
    /// for the specified logical expression.
    async fn derive_logical_properties(
        &self,
        group_id: GroupId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let logical_expr_id = self.memo.get_any_logical_expr(group_id).await?;
        let plan: PartialLogicalPlan = self
            .memo
            .materialize_logical_expr(logical_expr_id)
            .await?
            .into();

        let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());
        let engine_tx = self.engine_tx.clone();

        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    "derive",
                    vec![partial_logical_to_value(&plan)],
                    Arc::new(move |value| {
                        Box::pin(async move {
                            let properties = value_to_logical_properties(&value);
                            EngineMessageKind::NewProperties(group_id, properties)
                        })
                    }),
                )
                .await;

            Self::send_engine_response(job_id, engine_tx, response).await;
        });
        Ok(())
    }
}
