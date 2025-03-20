use std::sync::Arc;

use futures::{channel::mpsc, SinkExt};
use optd_dsl::analyzer::context::Context;

use super::{
    generator::OptimizerGenerator, memo::Memoize, tasks::TaskId, Optimizer, OptimizerMessage,
};
use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        properties::PhysicalProperties,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{Engine, LogicalExprContinuation, OptimizedExprContinuation},
};

/// Unique identifier for jobs in the optimization system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct JobId(pub i64);

/// A job represents a discrete unit of work within the optimization process
///
/// Jobs are launched by tasks and represent atomic operations that contribute to
/// completing the task. Multiple jobs may be launched by a single task, and all
/// jobs must complete before a task is considered (temporarily) finished.
pub(super) struct Job(pub TaskId, pub JobKind);

/// Enumeration of different types of jobs in the optimizer
///
/// Each variant represents a specific optimization operation that can be
/// performed asynchronously and independently.
pub(super) enum JobKind {
    /// Derives logical properties for a logical expression
    ///
    /// This job computes schema, cardinality estimates, and other
    /// statistical properties of a logical expression.
    DeriveLogicalProperties(DeriveLogicalPropertiesJob),

    /// Applies a transformation rule to a logical expression
    ///
    /// This job generates alternative logical expressions that are
    /// semantically equivalent to the original.
    TransformationRule(TransformationRuleJob),

    /// Applies an implementation rule to a logical expression and properties
    ///
    /// This job generates physical implementations of a logical expression
    /// based on specific implementation strategies.
    ImplementationRule(ImplementationRuleJob),

    /// Computes the cost of a physical expression
    ///
    /// This job estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan.
    CostExpression(CostExpressionJob),

    /// Continues processing with a logical expression result
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of a logical expression operation.
    ContinueWithLogical(ContinueWithLogicalJob),

    /// Continues processing with an optimized expression result
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of an optimized physical expression operation.
    ContinueWithOptimized(ContinueWithOptimizedJob),
}

impl JobKind {
    pub fn derive_logical_properties(logical_expr: LogicalExpression) -> Self {
        Self::DeriveLogicalProperties(DeriveLogicalPropertiesJob { logical_expr })
    }

    pub fn transformation_rule(
        rule_name: TransformationRule,
        logical_expr: LogicalExpression,
        group_id: GroupId,
    ) -> Self {
        Self::TransformationRule(TransformationRuleJob {
            rule_name,
            logical_expr,
            group_id,
        })
    }

    pub fn implementation_rule(
        rule_name: ImplementationRule,
        logical_expr: LogicalExpression,
        group_id: GroupId,
        physical_properties: PhysicalProperties,
    ) -> Self {
        Self::ImplementationRule(ImplementationRuleJob {
            rule_name,
            logical_expr,
            group_id,
            physical_properties,
        })
    }

    pub fn cost_expression(physical_expr: PhysicalExpression, goal: Goal) -> Self {
        Self::CostExpression(CostExpressionJob {
            physical_expr,
            goal,
        })
    }

    pub fn continue_with_logical(
        logical_expr: LogicalExpression,
        k: LogicalExprContinuation,
    ) -> Self {
        Self::ContinueWithLogical(ContinueWithLogicalJob { logical_expr, k })
    }

    pub fn continue_with_optimized(
        optimized_expr: OptimizedExpression,
        k: OptimizedExprContinuation,
    ) -> Self {
        Self::ContinueWithOptimized(ContinueWithOptimizedJob { optimized_expr, k })
    }
}

pub(super) struct DeriveLogicalPropertiesJob {
    pub logical_expr: LogicalExpression,
}

pub(super) struct TransformationRuleJob {
    rule_name: TransformationRule,
    logical_expr: LogicalExpression,
    group_id: GroupId,
}

pub(super) struct ImplementationRuleJob {
    rule_name: ImplementationRule,
    logical_expr: LogicalExpression,
    group_id: GroupId,
    physical_properties: PhysicalProperties,
}

pub(super) struct CostExpressionJob {
    physical_expr: PhysicalExpression,
    goal: Goal,
}

pub(super) struct ContinueWithLogicalJob {
    pub logical_expr: LogicalExpression,
    pub k: LogicalExprContinuation,
}

impl ContinueWithLogicalJob {}

pub(super) struct ContinueWithOptimizedJob {
    pub optimized_expr: OptimizedExpression,
    pub k: OptimizedExprContinuation,
}

/// Job-related implementation for the Optimizer
impl<M: Memoize> Optimizer<M> {
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

    fn new_engine(&self, job_id: JobId) -> Engine<OptimizerGenerator> {
        Engine::new(
            Context::default(),
            OptimizerGenerator::new(self.message_tx.clone(), job_id),
        )
    }

    pub(super) async fn derive_logical_properties_job(
        &mut self,
        job: DeriveLogicalPropertiesJob,
        job_id: JobId,
    ) {
        let DeriveLogicalPropertiesJob { logical_expr } = job;

        let engine = self.new_engine(job_id);
        let message_tx = self.message_tx.clone();
        engine
            .launch_derive_properties(
                &logical_expr.clone().into(),
                Arc::new(move |logical_props| {
                    let mut message_tx = message_tx.clone();
                    let logical_expr = logical_expr.clone();
                    Box::pin(async move {
                        message_tx
                            .send(OptimizerMessage::CreateGroup(
                                logical_props,
                                logical_expr,
                                job_id,
                            ))
                            .await
                            .unwrap();
                    })
                }),
            )
            .await;
    }

    pub(super) async fn execute_transformation_rule_job(
        &mut self,
        job: TransformationRuleJob,
        job_id: JobId,
    ) {
        let TransformationRuleJob {
            rule_name,
            logical_expr,
            group_id,
        } = job;

        let engine = self.new_engine(job_id);
        let message_tx = self.message_tx.clone();
        engine
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
            .await;
    }

    pub(super) async fn execute_implementation_rule_job(
        &mut self,
        job: ImplementationRuleJob,
        job_id: JobId,
    ) {
        let ImplementationRuleJob {
            rule_name: implementation_rule,
            logical_expr,
            group_id,
            physical_properties,
        } = job;

        let engine = self.new_engine(job_id);
        let message_tx = self.message_tx.clone();

        engine
            .launch_implementation_rule(
                implementation_rule.0,
                &logical_expr.into(),
                &physical_properties.clone(),
                Arc::new(move |partial_physical_plan| {
                    let mut message_tx = message_tx.clone();
                    let goal = Goal(group_id, physical_properties.clone());
                    Box::pin(async move {
                        message_tx
                            .send(OptimizerMessage::NewPhysicalPartial(
                                partial_physical_plan,
                                goal,
                                job_id,
                            ))
                            .await
                            .unwrap();
                    })
                }),
            )
            .await;
    }

    pub(super) async fn execute_cost_expression_job(
        &mut self,
        job: CostExpressionJob,
        job_id: JobId,
    ) {
        let CostExpressionJob {
            physical_expr,
            goal,
        } = job;

        let engine = self.new_engine(job_id);
        let message_tx = self.message_tx.clone();

        engine
            .launch_cost_plan(
                &physical_expr.clone().into(),
                Arc::new(move |cost| {
                    let mut message_tx = message_tx.clone();
                    let goal = goal.clone();
                    let physical_expr = physical_expr.clone();
                    Box::pin(async move {
                        message_tx
                            .send(OptimizerMessage::NewOptimizedExpression(
                                OptimizedExpression(physical_expr, cost),
                                goal,
                                job_id,
                            ))
                            .await
                            .unwrap();
                    })
                }),
            )
            .await;
    }

    pub(super) async fn execute_continue_with_logical_job(&mut self, job: ContinueWithLogicalJob) {
        let ContinueWithLogicalJob { logical_expr, k } = job;
        k(logical_expr).await;
    }

    pub(super) async fn execute_continue_with_optimized_job(
        &mut self,
        job: ContinueWithOptimizedJob,
    ) {
        let ContinueWithOptimizedJob { optimized_expr, k } = job;
        k(optimized_expr).await;
    }
}
