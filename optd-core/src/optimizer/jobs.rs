use super::tasks::{ImplementExpressionTask, TaskKind, TransformExpressionTask};
use super::{EngineMessage, Task};
use super::{EngineMessageKind, Optimizer, tasks::TaskId};
use crate::bridge::from_cir::{
    costed_physical_to_value, partial_logical_to_value, partial_physical_to_value,
    physical_properties_to_value,
};
use crate::bridge::into_cir::{
    hir_goal_to_cir, hir_group_id_to_cir, value_to_cost, value_to_logical_properties,
    value_to_partial_logical, value_to_partial_physical,
};
use crate::cir::{
    Cost, Goal, GoalId, GroupId, ImplementationRule, LogicalExpressionId, PartialLogicalPlan,
    PhysicalExpressionId, TransformationRule,
};
use crate::error::Error;
use crate::memo::Memoize;
use EngineMessageKind::*;
use JobKind::*;
use TaskKind::*;
use futures::SinkExt;
use futures::channel::mpsc::Sender;
use optd_dsl::analyzer::hir::Value;
use optd_dsl::engine::{Continuation, Engine, EngineResponse};
use std::sync::Arc;

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
    ContinueWithLogical(
        LogicalExpressionId,
        Continuation<Value, EngineResponse<EngineMessageKind>>,
    ),

    /// Continues processing with an optimized expression result.
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of an optimized physical expression operation.
    ContinueWithCostedPhysical(
        PhysicalExpressionId,
        Cost,
        Continuation<Value, EngineResponse<EngineMessageKind>>,
    ),
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

    pub(super) async fn send_engine_response(
        job_id: JobId,
        mut message_tx: Sender<EngineMessage>,
        response: EngineResponse<EngineMessageKind>,
    ) {
        match response {
            EngineResponse::Return(value, k) => {
                let msg = EngineMessage::new(job_id, k(value).await);
                message_tx.send(msg).await.unwrap();
            }
            EngineResponse::YieldGroup(group_id, k) => {
                let msg =
                    EngineMessage::new(job_id, SubscribeGroup(hir_group_id_to_cir(&group_id), k));
                message_tx.send(msg).await.unwrap();
            }
            EngineResponse::YieldGoal(goal, k) => {
                let msg = EngineMessage::new(job_id, SubscribeGoal(hir_goal_to_cir(&goal), k));
                message_tx.send(msg).await.unwrap();
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
        expression_id: LogicalExpressionId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let engine = Engine::new(self.hir_context.clone());

        let plan: PartialLogicalPlan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();

        tokio::spawn(async move {
            let logical_expression_id = expression_id;
            let response = engine
                .launch_rule(
                    "derive",
                    vec![partial_logical_to_value(&plan)],
                    Arc::new(move |value| {
                        Box::pin(async move {
                            let properties = value_to_logical_properties(&value);
                            // TODO(yuchen): refactor EngineMessage type to include job id in header instead.
                            CreateGroup(logical_expression_id, properties)
                        })
                    }),
                )
                .await;

            Self::send_engine_response(job_id, message_tx, response).await;
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
        let engine = Engine::new(self.hir_context.clone());

        let plan: PartialLogicalPlan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    &rule_name.0,
                    vec![partial_logical_to_value(&plan)],
                    Arc::new(move |value| {
                        let plan = value_to_partial_logical(&value);

                        Box::pin(async move { NewLogicalPartial(plan, group_id) })
                    }),
                )
                .await;

            Self::send_engine_response(job_id, message_tx, response).await;
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
        let engine = Engine::new(self.hir_context.clone());

        let Goal(_, physical_props) = self.memo.materialize_goal(goal_id).await?;
        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    &rule_name.0,
                    vec![
                        partial_logical_to_value(&plan),
                        physical_properties_to_value(&physical_props),
                    ],
                    Arc::new(move |value| {
                        let plan = value_to_partial_physical(&value);
                        Box::pin(async move { NewPhysicalPartial(plan, goal_id) })
                    }),
                )
                .await;
            Self::send_engine_response(job_id, message_tx, response).await;
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
        let engine = Engine::new(self.hir_context.clone());

        let plan = self.egest_partial_plan(expression_id).await?;

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    "cost",
                    vec![partial_physical_to_value(&plan)],
                    Arc::new(move |value| {
                        let cost = value_to_cost(&value);
                        Box::pin(async move { NewCostedPhysical(expression_id, cost) })
                    }),
                )
                .await;

            Self::send_engine_response(job_id, message_tx, response).await;
        });

        Ok(())
    }

    /// Executes a job to continue processing with a logical expression result.
    ///
    /// This materializes the logical expression and passes it to the continuation.
    async fn execute_continue_with_logical(
        &self,
        expression_id: LogicalExpressionId,
        k: Continuation<Value, EngineResponse<EngineMessageKind>>,
    ) -> Result<(), Error> {
        let plan = self.memo.materialize_logical_expr(expression_id).await?;

        tokio::spawn(async move {
            k(partial_logical_to_value(&plan.into())).await;
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
        k: Continuation<Value, EngineResponse<EngineMessageKind>>,
    ) -> Result<(), Error> {
        let plan = self.egest_partial_plan(physical_expr_id).await?;
        let costed_plan_value = costed_physical_to_value(plan, cost);

        tokio::spawn(async move {
            k(costed_plan_value).await;
        });

        Ok(())
    }
}
