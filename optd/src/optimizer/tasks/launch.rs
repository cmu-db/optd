use super::{
    CostExpressionTask, ImplementExpressionTask, OptimizePlanTask, Task, TaskId, TaskKind,
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
use tokio::sync::mpsc::{self, Receiver, Sender};

impl<M: Memo> Optimizer<M> {
    /// Creates a new task to optimize a logical plan into a physical plan.
    ///
    /// This function only allocates a task ID and does not yet launch the task
    /// until we know the corresponding goal that has to be optimized.
    pub(crate) fn create_optimize_plan_task(&mut self) -> TaskId {
        self.next_task_id()
    }

    /// Launches the task to optimize a logical plan into a physical plan.
    ///
    /// This function assumes the goal_id is known and that the logical plan has been
    /// properly ingested.
    ///
    /// # Parameters
    ///
    /// * `task_id`: The ID of the task to be launched.
    /// * `plan`: The logical plan to be optimized.
    /// * `response_tx`: The channel to send the optimized physical plan.
    /// * `goal_id`: The goal ID that this task is optimizing for.
    pub(crate) async fn launch_optimize_plan_task(
        &mut self,
        task_id: TaskId,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
        goal_id: GoalId,
    ) -> Result<(), OptimizeError> {
        use TaskKind::*;

        // Launch goal optimize task if needed, and get its ID.
        let goal_optimize_task_id = self.ensure_goal_optimize_task(goal_id).await?;
        let goal_optimize_task = self
            .get_optimize_goal_task_mut(goal_optimize_task_id)
            .unwrap();

        // Register task and connect in graph.
        let optimize_plan_task = OptimizePlanTask {
            plan,
            response_tx,
            optimize_goal_in: goal_optimize_task_id,
        };

        goal_optimize_task.optimize_plan_out.insert(task_id);
        self.add_task(task_id, Task::new(OptimizePlan(optimize_plan_task)));

        Ok(())
    }

    /// Ensures a group exploration task exists and returns its id.
    /// as part of its work. If an exploration task already exists, we reuse it.
    ///
    /// # Parameters
    ///
    /// * `group_id`: The ID of the group to be explored.
    ///
    /// # Returns
    ///
    /// * `TaskId`: The ID of the task that was created or reused.
    pub(crate) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }

    /// Ensures a goal optimization task exists and and returns its id.
    /// If an optimization task already exists, we reuse it.
    ///
    /// # Parameters
    ///
    /// * `goal_id`: The ID of the goal to be optimized.
    ///
    /// # Returns
    ///
    /// * `TaskId`: The ID of the task that was created or reused.
    pub(crate) async fn ensure_goal_optimize_task(
        &mut self,
        goal_id: GoalId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }

    /// Ensures a cost expression task exists and and returns its id.
    /// If a costing task already exists, we reuse it.
    ///
    /// # Parameters
    ///
    /// * `expression_id`: The ID of the expression to be costed.
    ///
    /// # Returns
    ///
    /// * `TaskId`: The ID of the task that was created or reused.
    pub(crate) async fn ensure_cost_expression_task(
        &mut self,
        expression_id: PhysicalExpressionId,
    ) -> Result<TaskId, OptimizeError> {
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
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }
}
