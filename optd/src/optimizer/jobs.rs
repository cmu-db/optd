use super::tasks::{ImplementExpressionTask, TaskKind, TransformExpressionTask};
use super::Task;
use super::{
    generator::OptimizerGenerator, memo::Memoize, tasks::TaskId, Optimizer, OptimizerMessage,
};
use crate::{
    cir::{
        expressions::{LogicalExpressionId, PhysicalExpressionId},
        goal::{Cost, Goal, GoalId},
        group::GroupId,
        operators::Child,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{CostedPhysicalPlanContinuation, Engine, LogicalPlanContinuation},
    error::Error,
};
use futures::SinkExt;
use std::sync::Arc;
use JobKind::*;
use OptimizerMessage::*;
use TaskKind::*;

/// Unique identifier for jobs in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct JobId(pub i64);

/// A job represents a discrete unit of work within the optimization process.
///
/// Jobs are launched by tasks and represent atomic operations that contribute to
/// completing the task. Multiple jobs may be launched by a single task, and all
/// jobs must complete before a task is considered (temporarily) finished.
#[derive(Clone)]
pub(super) struct Job(pub TaskId, pub JobKind);

/// Enumeration of different types of jobs in the optimizer.
///
/// Each variant represents a specific optimization operation that can be
/// performed asynchronously and independently.
#[derive(Clone)]
pub(super) enum JobKind {
    /// Derives logical properties for a logical expression.
    ///
    /// This job computes schema, cardinality estimates, and other
    /// statistical properties of a logical expression.
    DeriveLogicalProperties(LogicalExpressionId),

    /// Starts applying a transformation rule to a logical expression.
    ///
    /// This job generates alternative logical expressions that are
    /// semantically equivalent to the original.
    StartTransformationRule(TransformationRule, LogicalExpressionId, GroupId),

    /// Starts applying an implementation rule to a logical expression and properties.
    ///
    /// This job generates physical implementations of a logical expression
    /// based on specific implementation strategies.
    StartImplementationRule(ImplementationRule, LogicalExpressionId, GoalId),

    /// Starts computing the cost of a physical expression.
    ///
    /// This job estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan.
    StartCostExpression(PhysicalExpressionId),

    /// Continues processing with a logical expression result.
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of a logical expression operation.
    ContinueWithLogical(LogicalExpressionId, LogicalPlanContinuation),

    /// Continues processing with an optimized expression result.
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of an optimized physical expression operation.
    ContinueWithCostedPhysical(PhysicalExpressionId, Cost, CostedPhysicalPlanContinuation),
}

impl<M: Memoize> Optimizer<M> {
    //
    // Job Scheduling and Management
    //

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
    pub(super) fn schedule_job(&mut self, task_id: TaskId, kind: JobKind) -> JobId {
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
    /// Jobs are launched in FIFO order from the job schedule queue if the number
    /// of currently running jobs is below the maximum concurrent jobs limit.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during job launching.
    pub(super) async fn launch_pending_jobs(&mut self) -> Result<(), Error> {
        // Launch jobs only if we're below the maximum concurrent jobs limit, in FIFO order.
        while self.running_jobs.len() < self.max_concurrent_jobs
            && !self.job_schedule_queue.is_empty()
        {
            let job_id = self.job_schedule_queue.pop_front().unwrap();

            // Move the job from pending to running.
            let job = self.pending_jobs.remove(&job_id).unwrap();
            self.running_jobs.insert(job_id, job.clone());

            // Dispatch & execute the job in a new co-routine.
            match job.1 {
                DeriveLogicalProperties(logical_expr_id) => {
                    self.derive_logical_properties(logical_expr_id, job_id)
                        .await?;
                }
                StartTransformationRule(rule_name, logical_expr_id, group_id) => {
                    self.execute_transformation_rule(rule_name, logical_expr_id, group_id, job_id)
                        .await?;
                }
                StartImplementationRule(rule_name, expression_id, goal_id) => {
                    self.execute_implementation_rule(rule_name, expression_id, goal_id, job_id)
                        .await?;
                }
                StartCostExpression(expression_id) => {
                    self.execute_cost_expression(expression_id, job_id).await?;
                }
                ContinueWithLogical(logical_expr_id, k) => {
                    self.execute_continue_with_logical(logical_expr_id, k)
                        .await?;
                }
                ContinueWithCostedPhysical(expression_id, cost, k) => {
                    self.execute_continue_with_optimized(expression_id, cost, k)
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
    pub(super) async fn complete_job(&mut self, job_id: JobId) -> Result<(), Error> {
        // Remove the job from the running jobs.
        let Job(task_id, _) = self.running_jobs.remove(&job_id).unwrap();

        // Remove the job from the task's uncompleted jobs set.
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.uncompleted_jobs.remove(&job_id);

        // If the task has no uncompleted jobs, mark it as clean.
        if task.uncompleted_jobs.is_empty() {
            match &task.kind {
                ImplementExpression(ImplementExpressionTask {
                    rule,
                    expression_id,
                    goal_id,
                    ..
                }) => {
                    self.memo
                        .set_implementation_clean(*expression_id, *goal_id, rule)
                        .await?;
                }
                TransformExpression(TransformExpressionTask {
                    expression_id,
                    rule,
                    ..
                }) => {
                    self.memo
                        .set_transformation_clean(*expression_id, rule)
                        .await?;
                }
                CostExpression(task) => {
                    self.memo.set_cost_clean(task.expression_id).await?;
                }
                _ => {} // We don't track status for the other task kinds.
            }
        }

        // TODO(Alexis): Cleanup the parentless tasks to free up resources.

        Ok(())
    }

    /// Retrieves the task associated with a specific job.
    ///
    /// # Parameters
    /// * `job_id` - The ID of the job to find the related task for.
    ///
    /// # Returns
    /// * `Option<&Task>` - The task associated with the job, if found.
    pub(super) fn get_related_task(&self, job_id: JobId) -> Option<&Task> {
        let Job(task_id, _) = self.running_jobs.get(&job_id).unwrap();
        self.tasks.get(task_id)
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
        expression_id: LogicalExpressionId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );

        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_derive_properties(
                    plan,
                    Arc::new(move |logical_props| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(CreateGroup(expression_id, logical_props, job_id))
                                .await
                                .expect("Failed to send create group - channel closed");
                        })
                    }),
                )
                .await;
        });
        Ok(())
    }

    /// Executes a job to apply a transformation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the transformation rule
    /// application process for the specified logical expression.
    async fn execute_transformation_rule(
        &self,
        rule_name: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );

        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_transformation_rule(
                    rule_name.0,
                    plan,
                    Arc::new(move |plan| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(NewLogicalPartial(plan, group_id, job_id))
                                .await
                                .expect("Failed to send new logical partial - channel closed");
                        })
                    }),
                )
                .await;
        });
        Ok(())
    }

    /// Executes a job to apply an implementation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the implementation rule
    /// application process for the specified logical expression and goal.
    async fn execute_implementation_rule(
        &self,
        rule_name: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );

        let Goal(_, physical_props) = self.memo.materialize_goal(goal_id).await?;
        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_implementation_rule(
                    rule_name.0,
                    plan,
                    physical_props,
                    Arc::new(move |plan| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(NewPhysicalPartial(plan, goal_id, job_id))
                                .await
                                .expect("Failed to send new physical partial - channel closed");
                        })
                    }),
                )
                .await;
        });

        Ok(())
    }

    /// Executes a job to compute the cost of a physical expression.
    ///
    /// This creates an engine instance and launches the cost calculation process
    /// for the specified physical expression.
    async fn execute_cost_expression(
        &self,
        expression_id: PhysicalExpressionId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );

        let plan = self.egest_partial_plan(expression_id).await?;

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_cost_plan(
                    plan,
                    Arc::new(move |cost| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(NewCostedPhysical(expression_id, cost, job_id))
                                .await
                                .expect("Failed to send new costed physical - channel closed");
                        })
                    }),
                )
                .await;
        });

        Ok(())
    }

    /// Executes a job to continue processing with a logical expression result.
    ///
    /// This materializes the logical expression and passes it to the continuation.
    async fn execute_continue_with_logical(
        &self,
        expression_id: LogicalExpressionId,
        k: LogicalPlanContinuation,
    ) -> Result<(), Error> {
        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        tokio::spawn(async move {
            k(plan).await;
        });

        Ok(())
    }

    /// Executes a job to continue processing with an optimized physical expression result.
    ///
    /// This materializes the physical expression and passes it along with its cost
    /// to the continuation.
    async fn execute_continue_with_optimized(
        &self,
        physical_expr_id: PhysicalExpressionId,
        cost: Cost,
        k: CostedPhysicalPlanContinuation,
    ) -> Result<(), Error> {
        let plan = self.egest_partial_plan(physical_expr_id).await?;

        tokio::spawn(async move {
            k((plan, cost)).await;
        });

        Ok(())
    }
}