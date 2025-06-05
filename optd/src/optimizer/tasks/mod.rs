use super::jobs::LogicalContinuation;
use crate::cir::{
    GoalId, GroupId, ImplementationRule, LogicalExpressionId, LogicalPlan, PhysicalPlan,
    TransformationRule,
};
use hashbrown::HashSet;
use tokio::sync::mpsc::Sender;

mod delete;
mod launch;
mod manage;

/// Unique identifier for tasks in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(pub i64);

/// A task represents a higher-level objective in the optimization process.
///
/// They represent structured, potentially hierarchical components of the
/// optimization process.
#[derive(Clone)]
pub(crate) enum Task {
    OptimizePlan(OptimizePlanTask),
    OptimizeGoal(OptimizeGoalTask),
    ExploreGroup(ExploreGroupTask),
    ImplementExpression(ImplementExpressionTask),
    TransformExpression(TransformExpressionTask),
    ForkLogical(ForkLogicalTask),
    ContinueWithLogical(ContinueWithLogicalTask),
}

//=============================================================================
// Task variant structs
//=============================================================================

/// Top-level task to optimize a logical plan.
#[derive(Clone)]
pub(crate) struct OptimizePlanTask {
    /// The logical plan to be optimized.
    pub plan: LogicalPlan,

    /// Channel to send the optimized physical plans back.
    pub physical_tx: Sender<PhysicalPlan>,

    /// The only dependency to get the best plans from.
    pub optimize_goal_in: Option<TaskId>, // Some once the input plan is ingested.
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

    // Input tasks that feed this task.
    /// `OptimizeGoalTask` member (children) goals producing for this goal.
    pub optimize_goal_in: HashSet<TaskId>,
    /// `ForkLogicalTask` the corresponding group exploration task producing
    /// logical expressions.
    pub explore_group_in: TaskId,
    /// `ImplementExpressionTask` rules that are implementing logical expressions.
    pub implement_expression_in: HashSet<TaskId>,
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
    /// NOTE: Variable not used but kept for observability.
    pub _rule: TransformationRule,
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
pub(crate) struct ImplementExpressionTask {
    /// The implementation rule to apply.
    /// NOTE: Variable not used but kept for observability.
    pub _rule: ImplementationRule,
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

/// Task to fork the logical optimization process.
#[derive(Clone)]
pub(crate) struct ForkLogicalTask {
    /// The fork continuation.
    pub continuation: LogicalContinuation,

    /// `ContinueWithLogicalTask` | `TransformExpressionTask
    /// | `ImplementExpressionTask` that gets fed by the output of
    /// this task.
    /// NOTE: This variable is not used be kept for consistency.
    pub _out: TaskId,

    // Input tasks that feed this task.
    /// `ExploreGroupTask` the corresponding group exploration task producing
    /// logical expressions.
    pub explore_group_in: TaskId,
    /// `ContinueWithLogical` tasks spawned off and producing for this task.
    pub continue_with_logical_in: HashSet<TaskId>,
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

impl Task {
    /// Returns a string representation of the task type for logging purposes.
    pub(crate) fn task_type(&self) -> &'static str {
        match self {
            Task::OptimizePlan(_) => "OptimizePlan",
            Task::OptimizeGoal(_) => "OptimizeGoal",
            Task::ExploreGroup(_) => "ExploreGroup",
            Task::ImplementExpression(_) => "ImplementExpression",
            Task::TransformExpression(_) => "TransformExpression",
            Task::ForkLogical(_) => "ForkLogical",
            Task::ContinueWithLogical(_) => "ContinueWithLogical",
        }
    }
}
