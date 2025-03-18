use super::{memo::Memoize, tasks::TaskId, Optimizer};
use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        properties::PhysicalProperties,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{LogicalExprContinuation, OptimizedExprContinuation},
};

/// Unique identifier for jobs in the optimization system
pub(super) type JobId = i64;

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
    DeriveLogicalProperties(LogicalExpression),

    /// Applies a transformation rule to a logical expression
    ///
    /// This job generates alternative logical expressions that are
    /// semantically equivalent to the original.
    LaunchTransformationRule(TransformationRule, LogicalExpression),

    /// Applies an implementation rule to a logical expression and properties
    ///
    /// This job generates physical implementations of a logical expression
    /// based on specific implementation strategies.
    LaunchImplementationRule(ImplementationRule, LogicalExpression, PhysicalProperties),

    /// Computes the cost of a physical expression
    ///
    /// This job estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan.
    LaunchCostExpression(PhysicalExpression),

    /// Continues processing with a logical expression result
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of a logical expression operation.
    ContinueWithLogical(LogicalExpression, LogicalExprContinuation),

    /// Continues processing with an optimized expression result
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of an optimized physical expression operation.
    ContinueWithOptimized(OptimizedExpression, OptimizedExprContinuation),
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
        self.next_job_id += 1;

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
}
