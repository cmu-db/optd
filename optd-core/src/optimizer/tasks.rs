use super::jobs::JobId;
use crate::{
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        plans::LogicalPlan,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{LogicalExprContinuation, OptimizedExprContinuation},
};
use std::collections::{HashMap, HashSet};

/// Unique identifier for tasks in the optimization system
pub(super) type TaskId = i64;

/// A task represents a higher-level objective in the optimization process
///
/// Tasks are composed of one or more jobs and may depend on other tasks.
/// They represent structured, potentially hierarchical components of the
/// optimization process.
pub(super) struct Task {
    /// Tasks that depend on this task to complete
    pub children: Vec<TaskId>,

    /// The specific kind of task
    pub kind: TaskKind,

    /// Set of job IDs that must complete before this task is (temporarily) finished
    pub uncompleted_jobs: HashSet<JobId>,
}

/// Subscribers for a group in a transformation task
pub(super) struct TransformSubscribers {
    /// Map of group IDs to their continuations
    pub subscribers: HashMap<GroupId, Vec<LogicalExprContinuation>>,
}

/// Subscribers for a goal in an implementation task
pub(super) struct ImplementSubscribers {
    /// Map of group IDs to their continuations
    pub subscribers: HashMap<GroupId, Vec<LogicalExprContinuation>>,
}

/// Subscribers for a goal in a cost evaluation task
pub(super) struct CostSubscribers {
    /// Map of goals to their continuations
    pub subscribers: HashMap<Goal, Vec<OptimizedExprContinuation>>,
}

/// Enumeration of different types of tasks in the optimizer
///
/// Each variant represents a structured component of the optimization process
/// that may launch multiple jobs and coordinate their execution.
pub(super) enum TaskKind {
    /// Top-level task to optimize a logical plan
    ///
    /// This task coordinates the overall optimization process, exploring
    /// alternative plans and selecting the best implementation.
    OptimizePlan(LogicalPlan),

    /// Task to explore implementations for a specific goal
    ///
    /// This task generates and evaluates physical implementations that
    /// satisfy the properties required by the goal.
    ExploreGoal(Goal),

    /// Task to explore expressions in a logical group
    ///
    /// This task generates alternative logical expressions within an
    /// equivalence class through rule application.
    ExploreGroup(GroupId),

    /// Task to apply a specific implementation rule to a logical expression
    ///
    /// This task generates physical implementations from a logical expression
    /// using a specified implementation strategy. It maintains a set of subscribers
    /// that will be notified of the implementation results.
    ImplementExpression(ImplementationRule, LogicalExpression, ImplementSubscribers),

    /// Task to apply a specific transformation rule to a logical expression
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of subscribers
    /// that will be notified of the transformation results.
    TransformExpression(TransformationRule, LogicalExpression, TransformSubscribers),

    /// Task to compute the cost of a physical expression
    ///
    /// This task estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan. It maintains a set of subscribers
    /// that will be notified of the costing results.
    CostExpression(PhysicalExpression, CostSubscribers),
}
