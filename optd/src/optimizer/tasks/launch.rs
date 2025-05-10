use super::{OptimizePlanTask, Task, TaskId, TaskKind};
use crate::{
    cir::{
        Goal, GoalId, GroupId, LogicalExpressionId, LogicalPlan, PhysicalExpressionId,
        PhysicalPlan, TransformationRule,
    },
    memo::Memo,
    optimizer::{
        Optimizer,
        errors::OptimizeError,
        jobs::{JobKind, LogicalContinuation},
        tasks::{
            ContinueWithLogicalTask, ExploreGroupTask, ForkLogicalTask, OptimizeGoalTask,
            TransformExpressionTask,
        },
    },
};
use std::collections::HashSet;
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
        let goal_optimize_task_id = self.ensure_optimize_goal_task(goal_id).await?;
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

    /// Launches the task to explore a group of logical expressions.
    ///
    /// # Parameters
    /// * `group_id`: The ID of the group to be explored.
    /// * `continuation`: The logical continuation to be used.
    /// * `parent_task_id`: The ID of the parent task.
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
                self.launch_continue_with_logical_task(
                    *expression_id,
                    fork_task_id,
                    continuation.clone(),
                )
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

    /// Helper method to launch a task for continuing with a logical expression.
    ///
    /// # Parameters
    /// * `expr_id`: The ID of the logical expression to continue with.
    /// * `fork_out`: The ID of the fork task that this continue task feeds into.
    /// * `continuation`: The logical continuation to be used.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the created continue task.
    fn launch_continue_with_logical_task(
        &mut self,
        expr_id: LogicalExpressionId,
        fork_out: TaskId,
        continuation: LogicalContinuation,
    ) -> TaskId {
        use TaskKind::*;

        let task_id = self.next_task_id();
        let task = ContinueWithLogicalTask {
            expr_id,
            fork_out,
            fork_in: None,
        };

        self.add_task(task_id, Task::new(ContinueWithLogical(task)));
        self.schedule_job(task_id, JobKind::ContinueWithLogical(expr_id, continuation));

        task_id
    }

    /// Helper method to launch a task for transforming a logical expression with a rule.
    ///
    /// # Parameters
    /// * `expr_id`: The ID of the logical expression to transform.
    /// * `rule`: The transformation rule to apply.
    /// * `explore_group_out`: The ID of the exploration task that this transformation feeds into.
    /// * `group_id`: The ID of the group that this transformation belongs to.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the created transform task.
    fn launch_transform_expression_task(
        &mut self,
        expr_id: LogicalExpressionId,
        rule: TransformationRule,
        explore_group_out: TaskId,
        group_id: GroupId,
    ) -> TaskId {
        use TaskKind::*;

        let task_id = self.next_task_id();
        let task = TransformExpressionTask {
            rule: rule.clone(),
            expression_id: expr_id,
            explore_group_out,
            fork_in: None,
        };

        self.add_task(task_id, Task::new(TransformExpression(task)));
        self.schedule_job(
            task_id,
            JobKind::TransformExpression(rule, expr_id, group_id),
        );

        task_id
    }

    /// Ensures a group exploration task exists and returns its id.
    /// If an exploration task already exists, we reuse it.
    ///
    /// This function does the following:
    /// 1. Finds the representative group for the given group ID
    /// 2. Checks if an exploration task already exists for this group
    /// 3. If not, creates a new exploration task and associated transform expression tasks
    ///
    /// # Parameters
    /// * `group_id`: The ID of the group to be explored.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the task that was created or reused.
    async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
    ) -> Result<TaskId, OptimizeError> {
        use TaskKind::*;

        // Find the representative group for the given group ID.
        let group_repr = self
            .memo
            .find_repr_group_id(group_id)
            .await
            .map_err(OptimizeError::MemoError)?;

        // Check if we already have an exploration task for this group.
        if let Some(task_id) = self.group_exploration_task_index.get(&group_repr) {
            return Ok(*task_id);
        }

        let exploration_task_id = self.next_task_id();

        // Create transform expression tasks for each logical expression and rule combination.
        let logical_expressions = self
            .memo
            .get_all_logical_exprs(group_repr)
            .await
            .map_err(OptimizeError::MemoError)?;
        let transformations: Vec<_> = self
            .rule_book
            .get_transformations()
            .iter()
            .cloned()
            .collect();

        let mut transform_expr_in = HashSet::new();
        for &expression_id in &logical_expressions {
            for rule in &transformations {
                let task_id = self.launch_transform_expression_task(
                    expression_id,
                    rule.clone(),
                    exploration_task_id,
                    group_id,
                );
                transform_expr_in.insert(task_id);
            }
        }

        // Create the exploration task.
        let exploration_task = ExploreGroupTask {
            group_id,
            optimize_goal_out: HashSet::new(),
            fork_logical_out: HashSet::new(),
            transform_expr_in,
        };

        // Register the task in the manager and index.
        self.add_task(
            exploration_task_id,
            Task::new(ExploreGroup(exploration_task)),
        );
        self.group_exploration_task_index
            .insert(group_repr, exploration_task_id);

        Ok(exploration_task_id)
    }

    /// Ensures a goal optimization task exists and and returns its id.
    /// If an optimization task already exists, we reuse it.
    ///
    /// # Parameters
    /// * `goal_id`: The ID of the goal to be optimized.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the task that was created or reused.
    async fn ensure_optimize_goal_task(
        &mut self,
        goal_id: GoalId,
    ) -> Result<TaskId, OptimizeError> {
        use TaskKind::*;

        // Find the representative group for the given goal ID.
        let goal_repr = self
            .memo
            .find_repr_goal_id(goal_id)
            .await
            .map_err(OptimizeError::MemoError)?;

        // Check if we already have an optimization task for this goal.
        if let Some(task_id) = self.goal_optimization_task_index.get(&goal_repr) {
            return Ok(*task_id);
        }

        let goal_optimize_task_id = self.next_task_id();

        // TODO(Alexis): Materialize the goal and only explore the group - for now.
        // This is sufficient to support logical->logical transformation.
        let Goal(group_id, _) = self
            .memo
            .materialize_goal(goal_id)
            .await
            .map_err(OptimizeError::MemoError)?;
        let explore_group_in = self.ensure_group_exploration_task(group_id).await?;

        let goal_optimize_task = OptimizeGoalTask {
            goal_id,
            optimize_plan_out: HashSet::new(),
            optimize_goal_out: HashSet::new(),
            fork_costed_out: HashSet::new(),
            optimize_goal_in: HashSet::new(),
            explore_group_in,
            implement_expression_in: HashSet::new(),
            cost_expression_in: HashSet::new(),
        };

        // Register the task in the manager and index.
        self.add_task(
            goal_optimize_task_id,
            Task::new(OptimizeGoal(goal_optimize_task)),
        );
        self.goal_optimization_task_index
            .insert(goal_repr, goal_optimize_task_id);

        Ok(goal_optimize_task_id)
    }

    /// Ensures a cost expression task exists and and returns its id.
    /// If a costing task already exists, we reuse it.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the expression to be costed.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the task that was created or reused.
    #[allow(dead_code)]
    async fn ensure_cost_expression_task(
        &mut self,
        _expression_id: PhysicalExpressionId,
    ) -> Result<TaskId, OptimizeError> {
        todo!()
    }
}
