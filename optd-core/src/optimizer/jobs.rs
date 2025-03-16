use super::{memo::Memoize, tasks::TaskId, Optimizer};
use crate::{
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
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

    /// Applies an implementation rule to a logical expression
    ///
    /// This job generates physical implementations of a logical expression
    /// based on specific implementation strategies.
    LaunchImplementationRule(ImplementationRule, LogicalExpression),

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
    ContinueWithOptimized(PhysicalExpression, OptimizedExprContinuation),
}

/// Job-related implementation for the Optimizer
impl<M: Memoize> Optimizer<M> {
    /// Creates a new job and associates it with a task
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
    pub(super) fn create_job(&mut self, task_id: TaskId, kind: JobKind) -> JobId {
        // Generate a new job ID
        let job_id = self.next_job_id;
        self.next_job_id += 1;

        // Create the job
        let job = Job(task_id, kind);

        // Add job to pending jobs
        self.pending_jobs.insert(job_id, job);

        // Add job to the schedule queue
        self.job_schedule_queue.push_back(job_id);

        // Add the job to the task's uncompleted_jobs set
        if let Some(task) = self.tasks.get_mut(&task_id) {
            task.uncompleted_jobs.insert(job_id);
        }

        job_id
    }

    /// Creates transformation jobs for a set of expressions
    ///
    /// For each combination of expression and transformation rule,
    /// creates a job to apply the rule to the expression.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task that will own these jobs
    /// * `expressions` - The logical expressions to transform
    pub(super) fn create_transformation_jobs(
        &mut self,
        task_id: TaskId,
        expressions: &[LogicalExpression],
    ) {
        // Get all transformation rules
        let transformations = self.rule_book.get_transformations().to_vec();

        // Create a job for each (expression, rule) pair
        for expr in expressions {
            for rule in &transformations {
                self.create_job(
                    task_id,
                    JobKind::LaunchTransformationRule(rule.clone(), expr.clone()),
                );
            }
        }
    }

    /// Creates implementation jobs for a set of expressions
    ///
    /// For each combination of expression and implementation rule,
    /// creates a job to apply the rule to the expression.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task that will own these jobs
    /// * `expressions` - The logical expressions to implement
    pub(super) fn create_implementation_jobs(
        &mut self,
        task_id: TaskId,
        expressions: &[LogicalExpression],
    ) {
        // Get all implementation rules
        let implementations = self.rule_book.get_implementations().to_vec();

        // Create a job for each (expression, rule) pair
        for expr in expressions {
            for rule in &implementations {
                self.create_job(
                    task_id,
                    JobKind::LaunchImplementationRule(rule.clone(), expr.clone()),
                );
            }
        }
    }

    /// Creates cost evaluation jobs for physical expressions
    ///
    /// For each physical expression, creates a job to compute its cost.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task that will own these jobs
    /// * `expressions` - The physical expressions to cost
    pub(super) fn create_cost_jobs(&mut self, task_id: TaskId, expressions: &[PhysicalExpression]) {
        // Create a job for each expression
        for expr in expressions {
            self.create_job(task_id, JobKind::LaunchCostExpression(expr.clone()));
        }
    }
}
