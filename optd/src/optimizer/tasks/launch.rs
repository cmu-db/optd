use super::{OptimizePlanTask, Task, TaskId, TaskKind};
use crate::{
    cir::{GoalId, GroupId, LogicalPlan, PhysicalExpressionId, PhysicalPlan},
    memo::Memo,
    optimizer::{
        Optimizer,
        errors::OptimizeError,
        jobs::LogicalContinuation,
        tasks::{ContinueWithLogicalTask, ForkLogicalTask},
    },
};
use tokio::sync::mpsc::Sender;

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

    pub(crate) async fn launch_fork_logical_task(
        &mut self,
        group_id: GroupId,
        continuation: LogicalContinuation,
        parent_task_id: TaskId,
    ) -> Result<(), OptimizeError> {
        use TaskKind::*;

        let explore_group_in = self.ensure_group_exploration_task(group_id).await?;
        let fork_task_id = self.next_task_id();

        // Spawn all continuations.
        let continue_with_logical_in = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .map_err(OptimizeError::MemoError)?
            .iter()
            .map(|expression_id| {
                let id = self.next_task_id();
                let task = ContinueWithLogicalTask {
                    expr_id: *expression_id,
                    fork_out: fork_task_id,
                    fork_in: None,
                };

                self.add_task(id, Task::new(ContinueWithLogical(task)));
                id
            })
            .collect();

        // Create the task and add it to the task manager.
        let fork_logical_task = ForkLogicalTask {
            continuation,
            out: parent_task_id,
            explore_group_in,
            continue_with_logical_in,
        };
        self.get_explore_group_task_mut(explore_group_in)
            .unwrap()
            .fork_logical_out
            .insert(fork_task_id);

        self.add_task(fork_task_id, Task::new(ForkLogical(fork_logical_task)));

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
    async fn ensure_group_exploration_task(
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
    async fn ensure_goal_optimize_task(
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
    async fn ensure_cost_expression_task(
        &mut self,
        _expression_id: PhysicalExpressionId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }
}
