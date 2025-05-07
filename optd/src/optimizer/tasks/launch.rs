use super::{
    CostExpressionTask, ImplementExpressionTask, OptimizePlanTask, TaskId, TaskKind,
    TransformExpressionTask,
};
use crate::{
    cir::{
        Goal, GoalId, GoalMemberId, GroupId, ImplementationRule, LogicalExpressionId, LogicalPlan,
        PhysicalExpressionId, PhysicalPlan, TransformationRule,
    },
    memo::Memo,
    optimizer::{Optimizer, errors::OptimizeError},
};
use futures::channel::mpsc::Sender;

impl<M: Memo> Optimizer<M> {
    /// Launches a new task to optimize a logical plan into a physical plan.
    ///
    /// This method creates and registers a task that will optimize the provided logical
    /// plan into a physical plan. The optimized plan will be sent back through the provided
    /// response channel every time a better plan is found.
    pub(super) async fn launch_optimize_plan_task(
        &mut self,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) -> TaskId {
        todo!()
    }

    /// Ensures a group exploration task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to explore all possible expressions in a group
    /// as part of its work. If an exploration task already exists, we reuse it.
    pub(super) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<(), OptimizeError> {
        todo!()
    }

    /// Ensures a goal optimization task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to optimize a goal as part of its work.
    /// If an optimization task already exists, we reuse it.
    pub(super) async fn ensure_goal_optimize_task(
        &mut self,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<(), OptimizeError> {
        todo!()
    }

    /// Ensures a cost expression task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to cost a physical expression as part of its work.
    /// If a costing task already exists, we reuse it.
    pub(super) async fn ensure_cost_expression_task(
        &mut self,
        expression_id: PhysicalExpressionId,
        parent_task_id: TaskId,
    ) -> Result<(), OptimizeError> {
        todo!()
    }

    //-------------------------------------------------------------------------
    // Internal task launching methods
    //-------------------------------------------------------------------------

    /// Launches a task to start applying a transformation rule to a logical expression.
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    async fn launch_transform_expression_task(
        &mut self,
        rule: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }

    /// Launches a task to start applying an implementation rule to a logical expression.
    ///
    /// This task generates physical implementations from a logical expression
    /// using a specified implementation strategy. It maintains a set of continuations
    /// that will be notified of the implementation results.
    ///
    /// Only schedules the starting job if the implementation is marked as dirty in the memo.
    async fn launch_implement_expression_task(
        &mut self,
        rule: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }

    /// Launches a new task to explore all possible transformations for a logical group.
    ///
    /// This schedules jobs to apply all available transformation rules to all
    /// logical expressions in the group.
    async fn launch_group_exploration_task(
        &mut self,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }

    /// Launches a new task to optimize a goal.
    ///
    /// This method creates and manages the tasks needed to optimize a goal by
    /// ensuring group exploration, launching implementation tasks, and processing goal members.
    async fn launch_goal_optimize_task(
        &mut self,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }
}
