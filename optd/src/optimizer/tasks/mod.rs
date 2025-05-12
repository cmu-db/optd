use super::{
    JobId,
    jobs::{CostedContinuation, LogicalContinuation},
};
use crate::cir::{
    Cost, GoalId, GroupId, ImplementationRule, LogicalExpressionId, LogicalPlan,
    PhysicalExpressionId, PhysicalPlan, TransformationRule,
};
use std::collections::HashSet;
use tokio::sync::mpsc::Sender;

mod delete;
mod launch;
mod manage;

/// Unique identifier for tasks in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(pub i64);

/// A task represents a higher-level objective in the optimization process.
///
/// Tasks are composed of one or more jobs and may depend on other tasks.
/// They represent structured, potentially hierarchical components of the
/// optimization process.
#[derive(Clone)]
pub(crate) struct Task {
    /// The specific kind of task.
    pub kind: TaskKind,

    /// Set of job IDs that is depending on.
    pub uncompleted_jobs: HashSet<JobId>,
}

/// Enumeration of different types of tasks in the optimizer.
///
/// Each variant represents a structured component of the optimization process
/// that may launch multiple jobs and coordinate their execution.
#[derive(Clone)]
pub(crate) enum TaskKind {
    OptimizePlan(OptimizePlanTask),
    OptimizeGoal(OptimizeGoalTask),
    ExploreGroup(ExploreGroupTask),
    #[allow(dead_code)]
    ImplementExpression(ImplementExpressionTask),
    TransformExpression(TransformExpressionTask),
    #[allow(dead_code)]
    CostExpression(CostExpressionTask),
    ForkLogical(ForkLogicalTask),
    #[allow(dead_code)]
    ForkCosted(ForkCostedTask),
    ContinueWithLogical(ContinueWithLogicalTask),
    #[allow(dead_code)]
    ContinueWithCosted(ContinueWithCostedTask),
}

//=============================================================================
// Task variant structs
//=============================================================================

/// Top-level task to optimize a logical plan.
#[derive(Clone)]
pub(crate) struct OptimizePlanTask {
    /// The logical plan to be optimized.
    pub plan: LogicalPlan,

    /// Channel to send the optimized physical plan back.
    pub response_tx: Sender<PhysicalPlan>,

    /// The only dependency to get the best plans from.
    pub optimize_goal_in: TaskId,
}

/// Task to optimize a specific goal.
#[derive(Clone)]
pub(crate) struct OptimizeGoalTask {
    /// The goal to optimize.
    pub goal_id: GoalId,

    // Output tasks that get fed by the output of this task.
    /// `OptimizePlanTask` source optimization requests.
    pub optimize_plan_out: HashSet<TaskId>,
    /// `OptimizeGoalTask` parent goals that this task is simultaneously
    /// producing for.
    pub optimize_goal_out: HashSet<TaskId>,
    /// `ForkCostedTask` subscribed to this goal.
    pub fork_costed_out: HashSet<TaskId>,

    // Input tasks that feed this task.
    /// `OptimizeGoalTask` member (children) goals producing for this goal.
    pub optimize_goal_in: HashSet<TaskId>,
    /// `ForkLogicalTask` the corresponding group exploration task producing
    /// logical expressions.
    pub explore_group_in: TaskId,
    /// `ImplementExpressionTask` rules that are implementing logical expressions.
    pub implement_expression_in: HashSet<TaskId>,
    /// `CostExpressionTask` costing of physical expressions.
    pub cost_expression_in: HashSet<TaskId>,
}

/// Task to explore expressions in a logical group.
#[derive(Clone)]
pub(crate) struct ExploreGroupTask {
    /// The group to explore.
    pub group_id: GroupId,
    /// All dispatched logical expressions in the group.
    pub dispatched_exprs: HashSet<LogicalExpressionId>,

    // Output tasks that get fed by the output of this task.
    /// `OptimizeGoalTask` optimization tasks depending on this group to get
    /// the required logical expressions.
    pub optimize_goal_out: HashSet<TaskId>,
    /// `ForkLogicalTask` subscribed to this group.
    pub fork_logical_out: HashSet<TaskId>,

    // Input tasks that feed this task.
    /// `TransformExpressionTask` rules that are exploring logical expressions.
    pub transform_expr_in: HashSet<TaskId>,
}

/// Task to apply a specific transformation rule to a logical expression.
#[derive(Clone)]
pub(crate) struct TransformExpressionTask {
    /// The transformation rule to apply.
    pub rule: TransformationRule,
    /// The logical expression to transform.
    pub expression_id: LogicalExpressionId,

    // Output tasks that get fed by the output of this task.
    /// `ExploreGroupTask` corresponding group exploration task.
    pub explore_group_out: TaskId,

    // Input tasks that feed this task.
    /// `ForkLogicalTask` logical fork points encountered during the
    /// transformation.
    pub fork_in: Option<TaskId>,
}

/// Task to implement a logical expression into a physical expression.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct ImplementExpressionTask {
    /// The implementation rule to apply.
    pub rule: ImplementationRule,
    /// The logical expression to implement.
    pub expression_id: LogicalExpressionId,

    // Output tasks that get fed by the output of this task.
    /// `OptimizeGoalTask` corresponding goal optimization task.
    pub optimize_goal_out: TaskId,

    // Input tasks that feed this task.
    /// `ForkLogicalTask` logical fork points encountered during the
    /// implementation.
    pub fork_in: Option<TaskId>,
}

/// Task to cost a physical expression.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CostExpressionTask {
    /// The physical expression to cost.
    pub expression_id: PhysicalExpressionId,
    /// The current upper bound on the allowed cost budget.
    pub budget: Cost,

    // Output tasks that get fed by the output of this task.
    /// `OptimizeGoalTask` corresponding goal optimization task.
    pub optimize_goal_out: HashSet<TaskId>,

    // Input tasks that feed this task.
    /// `ForkCostedTask` cost fork points encountered during the
    /// costing.
    pub fork_in: Option<TaskId>,
}

/// Task to fork the logical optimization process.
#[derive(Clone)]
pub(crate) struct ForkLogicalTask {
    /// The fork continuation.
    pub continuation: LogicalContinuation,

    /// `ContinueWithLogicalTask` | `TransformExpressionTask
    /// | `ImplementExpressionTask` that gets fed by the output of
    /// this task.
    pub out: TaskId,

    // Input tasks that feed this task.
    /// `ExploreGroupTask` the corresponding group exploration task producing
    /// logical expressions.
    pub explore_group_in: TaskId,
    /// `ContinueWithLogical` tasks spawned off and producing for this task.
    pub continue_with_logical_in: HashSet<TaskId>,
}

/// Task to fork the costed optimization process.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct ForkCostedTask {
    /// The fork continuation.
    pub continuation: CostedContinuation,
    /// The current upper bound on the allowed cost budget.
    pub budget: Cost,

    /// `ContinueWithCostedTask` | `CostExpressionTask` that gets fed by the
    /// output of this task.
    pub out: TaskId,

    // Input tasks that feed this task.
    /// `OptimizeGoalTask` corresponding goal optimization task producing
    /// costed expressions.
    pub optimize_goal_in: TaskId,
    /// `ContinueWithCosted` tasks spawned off and producing for this task.
    pub continue_with_costed_in: HashSet<TaskId>,
}

/// Task to continue with a logical expression.
#[derive(Clone)]
pub(crate) struct ContinueWithLogicalTask {
    /// The logical expression to continue with.
    pub expression_id: LogicalExpressionId,

    /// `ForkLogicalTask` that gets fed by the output of this continuation.
    pub fork_out: TaskId,
    /// Potential `ForkLogicalTask` fork spawned off from this task.
    pub fork_in: Option<TaskId>,
}

/// Task to continue with a costed expression.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct ContinueWithCostedTask {
    /// The physical expression to continue with.
    pub expression_id: PhysicalExpressionId,

    /// `ForkCostedTask` that gets fed by the output of this continuation.
    pub fork_out: TaskId,
    /// Potential `ForkCostedTask` fork spawned off from this task.
    pub fork_in: Option<TaskId>,
}

impl Task {
    /// Creates a new task with the specified kind and empty job set.
    fn new(kind: TaskKind) -> Self {
        Self {
            kind,
            uncompleted_jobs: HashSet::new(),
        }
    }
}
