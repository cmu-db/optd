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

use continue_with_costed::*;
use continue_with_logical::*;
use cost_expr::*;
use explore_group::*;
use fork_costed::*;
use fork_logical::*;
use implement_expr::*;
use optimize_goal::*;
use optimize_plan::*;
use transform_expr::*;

use crate::memo::Memoize;

use super::Optimizer;

/// Unique identifier for tasks in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub i64);

#[derive(Debug, Clone, Copy)]
pub enum SourceTaskId {
    /// Task to optimize a logical plan.
    OptimizePlan(TaskId),

    /// Task to optimize a specific goal.
    OptimizeGoal(TaskId),

    /// Task to explore expressions in a logical group.
    ExploreGroup(TaskId),

    /// Task to apply a specific implementation rule to a logical expression.
    ImplementExpression(TaskId),

    /// Task to apply a specific transformation rule to a logical expression.
    TransformExpression(TaskId),

    /// Task to compute the cost of a physical expression.
    CostExpression(TaskId),

    ForkLogical(TaskId),
    ContinueWithLogical(TaskId),
    ForkCosted(TaskId),
    ContinueWithCosted(TaskId),
}

/// A task represents a higher-level objective in the optimization process.
///
/// Tasks are composed of one or more jobs and may depend on other tasks.
/// They represent structured, potentially hierarchical components of the
/// optimization process.
#[derive(Debug)]
pub enum Task {
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

impl<M: Memoize> Optimizer<M> {
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.get(&task_id).unwrap()
    }
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.get_mut(&task_id).unwrap()
    }
}

impl Task {
    pub fn as_explore_group_mut(&mut self) -> &mut ExploreGroupTask {
        match self {
            Task::ExploreGroup(task) => task,
            _ => panic!("Expected ExploreGroup task"),
        }
    }
    pub fn as_optimize_goal_mut(&mut self) -> &mut OptimizeGoalTask {
        match self {
            Task::OptimizeGoal(task) => task,
            _ => panic!("Expected OptimizeGoal task"),
        }
    }
    pub fn as_optimize_plan_mut(&mut self) -> &mut OptimizePlanTask {
        match self {
            Task::OptimizePlan(task) => task,
            _ => panic!("Expected OptimizePlan task"),
        }
    }
    pub fn as_implement_expression_mut(&mut self) -> &mut ImplementExpressionTask {
        match self {
            Task::ImplementExpression(task) => task,
            _ => panic!("Expected ImplementExpression task"),
        }
    }
    pub fn as_transform_expression_mut(&mut self) -> &mut TransformExpressionTask {
        match self {
            Task::TransformExpression(task) => task,
            _ => panic!("Expected TransformExpression task"),
        }
    }
    pub fn as_cost_expression_mut(&mut self) -> &mut CostExpressionTask {
        match self {
            Task::CostExpression(task) => task,
            _ => panic!("Expected CostExpression task"),
        }
    }
    pub fn as_fork_logical_mut(&mut self) -> &mut ForkLogicalTask {
        match self {
            Task::ForkLogical(task) => task,
            _ => panic!("Expected ForkLogical task"),
        }
    }
    pub fn as_continue_with_logical_mut(&mut self) -> &mut ContinueWithLogicalTask {
        match self {
            Task::ContinueWithLogical(task) => task,
            _ => panic!("Expected ContinueWithLogical task"),
        }
    }
    pub fn as_fork_costed_mut(&mut self) -> &mut ForkCostedTask {
        match self {
            Task::ForkCosted(task) => task,
            _ => panic!("Expected ForkCosted task"),
        }
    }
    pub fn as_continue_with_costed_mut(&mut self) -> &mut ContinueWithCostedTask {
        match self {
            Task::ContinueWithCosted(task) => task,
            _ => panic!("Expected ContinueWithCosted task"),
        }
    }
    pub fn as_explore_group(&self) -> &ExploreGroupTask {
        match self {
            Task::ExploreGroup(task) => task,
            _ => panic!("Expected ExploreGroup task"),
        }
    }
    pub fn as_optimize_goal(&self) -> &OptimizeGoalTask {
        match self {
            Task::OptimizeGoal(task) => task,
            _ => panic!("Expected OptimizeGoal task"),
        }
    }
    pub fn as_optimize_plan(&self) -> &OptimizePlanTask {
        match self {
            Task::OptimizePlan(task) => task,
            _ => panic!("Expected OptimizePlan task"),
        }
    }
    pub fn as_implement_expression(&self) -> &ImplementExpressionTask {
        match self {
            Task::ImplementExpression(task) => task,
            _ => panic!("Expected ImplementExpression task"),
        }
    }
    pub fn as_transform_expression(&self) -> &TransformExpressionTask {
        match self {
            Task::TransformExpression(task) => task,
            _ => panic!("Expected TransformExpression task"),
        }
    }
    pub fn as_cost_expression(&self) -> &CostExpressionTask {
        match self {
            Task::CostExpression(task) => task,
            _ => panic!("Expected CostExpression task"),
        }
    }
    pub fn as_fork_logical(&self) -> &ForkLogicalTask {
        match self {
            Task::ForkLogical(task) => task,
            _ => panic!("Expected ForkLogical task"),
        }
    }
    pub fn as_continue_with_logical(&self) -> &ContinueWithLogicalTask {
        match self {
            Task::ContinueWithLogical(task) => task,
            _ => panic!("Expected ContinueWithLogical task"),
        }
    }
    pub fn as_fork_costed(&self) -> &ForkCostedTask {
        match self {
            Task::ForkCosted(task) => task,
            _ => panic!("Expected ForkCosted task"),
        }
    }
    pub fn as_continue_with_costed(&self) -> &ContinueWithCostedTask {
        match self {
            Task::ContinueWithCosted(task) => task,
            _ => panic!("Expected ContinueWithCosted task"),
        }
    }
}
