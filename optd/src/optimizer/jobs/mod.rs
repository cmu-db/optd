use super::{EngineProduct, TaskId};
use crate::{
    cir::{GoalId, GroupId, ImplementationRule, LogicalExpressionId, TransformationRule},
    dsl::{
        analyzer::hir::Value,
        engine::{Continuation, EngineResponse},
    },
};

pub(crate) mod execute;
pub(crate) mod manage;

/// Unique identifier for jobs in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct JobId(pub i64);

/// A job represents a discrete unit of work within the optimization process.
///
/// Jobs are launched by tasks and represent atomic operations that contribute to
/// completing the task. Multiple jobs may be launched by a single task, and all
/// jobs must complete before a task is considered (temporarily) finished.
#[derive(Clone)]
pub(crate) struct Job(pub TaskId, pub JobKind);

/// Represents a continuation for processing logical expressions.
#[derive(Clone)]
pub(crate) struct LogicalContinuation(Continuation<Value, EngineResponse<EngineProduct>>);

/// Enumeration of different types of jobs in the optimizer.
///
/// Each variant represents a specific optimization operation that can be
/// performed asynchronously and independently.
#[derive(Clone)]
pub(crate) enum JobKind {
    /// Derives logical properties for a logical expression.
    ///
    /// This job computes schema, cardinality estimates, and other
    /// statistical properties of a logical expression.
    Derive(LogicalExpressionId),

    /// Starts applying a transformation rule to a logical expression.
    ///
    /// This job generates alternative logical expressions that are
    /// semantically equivalent to the original.
    TransformExpression(TransformationRule, LogicalExpressionId, GroupId),

    /// Starts applying an implementation rule to a logical expression and properties.
    ///
    /// This job generates physical implementations of a logical expression
    /// based on specific implementation strategies.
    ImplementExpression(ImplementationRule, LogicalExpressionId, GoalId),

    /// Continues processing with a logical expression result.
    ///
    /// This job represents a continuation-passing-style callback for
    /// handling the result of a logical expression operation.
    ContinueWithLogical(LogicalExpressionId, LogicalContinuation),
}
