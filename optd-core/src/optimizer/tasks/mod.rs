mod continue_with_costed;
mod continue_with_logical;
mod cost_expr;
mod explore_group;
mod fork_costed;
mod fork_logical;
mod implement_expr;
mod optimize_goal;
mod optimize_plan;
mod transform_expr;

pub use continue_with_costed::*;
pub use continue_with_logical::*;
pub use cost_expr::*;
pub use explore_group::*;
pub use fork_costed::*;
pub use fork_logical::*;
pub use implement_expr::*;
pub use optimize_goal::*;
pub use optimize_plan::*;
pub use transform_expr::*;

/// Unique identifier for tasks in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TaskId(pub i64);

/// A task represents a higher-level objective in the optimization process.
///
/// Tasks are composed of one or more jobs and may depend on other tasks.
/// They represent structured, potentially hierarchical components of the
/// optimization process.
pub(super) enum Task {
    /// Top-level task to optimize a logical plan.
    OptimizePlan(OptimizePlanTask),

    /// Task to optimize a specific goal.
    OptimizeGoal(OptimizeGoalTask),

    /// Task to explore expressions in a logical group.
    ExploreGroup(ExploreGroupTask),

    /// Task to apply a specific implementation rule to a logical expression.
    ImplementExpression(ImplementExpressionTask),

    /// Task to apply a specific transformation rule to a logical expression.
    TransformExpression(TransformExpressionTask),

    /// Task to compute the cost of a physical expression.
    CostExpression(CostExpressionTask),

    ForkLogical(ForkLogicalTask),
    ContinueWithLogical(ContinueWithLogicalTask),
    ForkCosted(ForkCostedTask),
    ContinueWithCosted(ContinueWithCostedTask),
}
