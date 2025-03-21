use super::{generator::OptimizerGenerator, memo::Memoize, tasks::TaskId, Optimizer};
use crate::{
    cir::{
        expressions::{LogicalExpressionId, PhysicalExpressionId},
        goal::{Cost, GoalId},
        group::GroupId,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{CostedPhysicalPlanContinuation, Engine, LogicalPlanContinuation},
    error::Error,
};

/// Unique identifier for jobs in the optimization system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct JobId(pub i64);

/// A job represents a discrete unit of work within the optimization process
///
/// Jobs are launched by tasks and represent atomic operations that contribute to
/// completing the task. Multiple jobs may be launched by a single task, and all
/// jobs must complete before a task is considered (temporarily) finished.
#[derive(Clone)]
pub(super) struct Job(pub TaskId, pub JobKind);

/// Enumeration of different types of jobs in the optimizer
///
/// Each variant represents a specific optimization operation that can be
/// performed asynchronously and independently.
#[derive(Clone)]
pub(super) enum JobKind {
    /// Derives logical properties for a logical expression
    ///
    /// This job computes schema, cardinality estimates, and other
    /// statistical properties of a logical expression.
    DeriveLogicalProperties(LogicalExpressionId),

    /// Starts applying a transformation rule to a logical expression
    ///
    /// This job generates alternative logical expressions that are
    /// semantically equivalent to the original.
    StartTransformationRule(TransformationRule, LogicalExpressionId, GroupId),

    /// Starts applying an implementation rule to a logical expression and properties
    ///
    /// This job generates physical implementations of a logical expression
    /// based on specific implementation strategies.
    StartImplementationRule(ImplementationRule, LogicalExpressionId, GoalId),

    /// Starts computing the cost of a physical expression
    ///
    /// This job estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan.
    StartCostExpression(PhysicalExpressionId),

    /// Continues processing with a logical expression result
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of a logical expression operation.
    ContinueWithLogical(LogicalExpressionId, LogicalPlanContinuation),

    /// Continues processing with an optimized expression result
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of an optimized physical expression operation.
    ContinueWithCostedPhysical(PhysicalExpressionId, Cost, CostedPhysicalPlanContinuation),
}

impl<M: Memoize> Optimizer<M> {
    pub(super) async fn launch_pending_jobs(&mut self) -> Result<(), Error> {
        // Launch jobs only if we're below the maximum concurrent jobs limit, in FIFO order.
        while self.running_jobs.len() < self.max_concurrent_jobs
            && !self.job_schedule_queue.is_empty()
        {
            let job_id = match self.job_schedule_queue.pop_front() {
                Some(id) => id,
                None => break,
            };

            // Move the job from pending to running.
            let job = self.pending_jobs.remove(&job_id).unwrap();
            self.running_jobs.insert(job_id, job.clone());

            // Dispatch & execute the job in a new co-routine.
            match job.1 {
                JobKind::DeriveLogicalProperties(logical_expr_id) => {
                    self.derive_logical_properties_job(logical_expr_id, job_id);
                }
                JobKind::StartTransformationRule(rule_name, logical_expr_id, group_id) => {
                    self.execute_transformation_rule_job(
                        rule_name,
                        logical_expr_id,
                        group_id,
                        job_id,
                    );
                }
                JobKind::StartImplementationRule(rule_name, expression_id, goal_id) => {
                    self.execute_implementation_rule_job(
                        rule_name,
                        expression_id,
                        goal_id,
                        job_id,
                    )?;
                }
                JobKind::StartCostExpression(expression_id) => {
                    self.execute_cost_expression_job(expression_id, job_id);
                }
                JobKind::ContinueWithLogical(logical_expr_id, k) => {
                    self.execute_continue_with_logical_job(logical_expr_id, k);
                }
                JobKind::ContinueWithCostedPhysical(expression_id, cost, k) => {
                    self.execute_continue_with_optimized_job(expression_id, cost, k);
                }
            }
        }

        Ok(())
    }

    /// Schedules a new job and associates it with a task
    ///
    /// This method creates a job of the specified kind, associates it with
    /// the given task, adds it to the pending jobs collection, and updates
    /// the task's uncompleted jobs set.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task that's launching this job
    /// * `kind` - The kind of job to create
    ///
    /// # Returns
    /// The ID of the created job
    pub(super) fn schedule_job(&mut self, task_id: TaskId, kind: JobKind) -> JobId {
        // Generate a new job ID
        let job_id = self.next_job_id;
        self.next_job_id.0 += 1;

        // Create & schedule the job
        let job = Job(task_id, kind);
        self.pending_jobs.insert(job_id, job);
        self.job_schedule_queue.push_back(job_id);

        // Add job to task's uncompleted jobs set
        self.tasks
            .get_mut(&task_id)
            .expect("Task must exist when creating a job for it")
            .uncompleted_jobs
            .insert(job_id);

        job_id
    }

    fn derive_logical_properties_job(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        job_id: JobId,
    ) {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );

        let message_tx = self.message_tx.clone();

        // Spawn inside the function, not in launch_pending_jobs
        tokio::spawn(async move {
            // TODO: Materialize here!
            /*engine
            .launch_derive_properties(
                &logical_expr.clone().into(),
                Arc::new(move |logical_props| {
                    let mut message_tx = message_tx.clone();
                    let logical_expr = logical_expr.clone();
                    Box::pin(async move {
                        message_tx
                            .send(OptimizerMessage::CreateGroup(
                                logical_expr_id,
                                logical_props,
                                job_id,
                            ))
                            .await
                            .unwrap();
                    })
                }),
            )
            .await;*/
        });
    }

    fn execute_transformation_rule_job(
        &mut self,
        rule_name: TransformationRule,
        logical_expr_id: LogicalExpressionId,
        group_id: GroupId,
        job_id: JobId,
    ) {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );
        let message_tx = self.message_tx.clone();

        // TODO: Materialize here!
        tokio::spawn(async move {
            /*engine
            .launch_transformation_rule(
                rule_name.0,
                &logical_expr.into(),
                Arc::new(move |partial_logical_plan| {
                    let mut message_tx = message_tx.clone();
                    Box::pin(async move {
                        message_tx
                            .send(OptimizerMessage::NewLogicalPartial(
                                partial_logical_plan,
                                group_id,
                                job_id,
                            ))
                            .await
                            .unwrap();
                    })
                }),
            )
            .await;*/
        });
    }

    fn execute_implementation_rule_job(
        &mut self,
        rule_name: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );
        let message_tx = self.message_tx.clone();

        // TODO: Materialize here!
        tokio::spawn(async move {
            /*engine
                    .launch_implementation_rule(
                        rule_name.0,
                        &logical_expr.into(),
                        &physical_properties.clone(),
                        Arc::new(move |partial_physical_plan| {
                            let mut message_tx = message_tx.clone();
                            Box::pin(async move {
                                message_tx
                                    .send(OptimizerMessage::NewPhysicalPartial(
                                        partial_physical_plan,
                                        goal_id,
                                        job_id,
                                    ))
                                    .await
                                    .unwrap();
                            })
                        }),
                    )
                    .await;
            */
        });

        Ok(())
    }

    fn execute_cost_expression_job(&mut self, expression_id: PhysicalExpressionId, job_id: JobId) {
        let engine = Engine::new(
            self.hir_context.clone(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        );
        let message_tx = self.message_tx.clone();

        // TODO: Will need to materialize here!
        tokio::spawn(async move {
            /*engine
            .launch_cost_plan(
                &physical_expr.clone().into(),
                Arc::new(move |cost| {
                    let mut message_tx = message_tx.clone();
                    let physical_expr = physical_expr.clone();
                    Box::pin(async move {
                        message_tx
                            .send(OptimizerMessage::NewCostedPhysical(
                                OptimizedExpression(physical_expr, cost),
                                goal_id,
                                job_id,
                            ))
                            .await
                            .unwrap();
                    })
                }),
            )
            .await;*/
        });
    }

    fn execute_continue_with_logical_job(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        k: LogicalPlanContinuation,
    ) {
        // TODO: Will need to materialize here!
        tokio::spawn(async move {
            // k(logical_expr).await;
        });
    }

    fn execute_continue_with_optimized_job(
        &mut self,
        expression_id: PhysicalExpressionId,
        cost: Cost,
        k: CostedPhysicalPlanContinuation,
    ) {
        // TODO: Will need to materialize here!
        tokio::spawn(async move {
            // k(optimized_expr).await;
        });
    }
}
