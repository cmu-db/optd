use super::{OptimizePlanTask, Task, TaskId};
use crate::{
    cir::*,
    memo::Memo,
    optimizer::{
        Optimizer,
        jobs::{JobKind, LogicalContinuation},
        tasks::{
            ContinueWithLogicalTask, ExploreGroupTask, ForkLogicalTask, ImplementExpressionTask,
            OptimizeGoalTask, TransformExpressionTask,
        },
    },
};
use async_recursion::async_recursion;
use hashbrown::HashSet;
use tokio::sync::mpsc::Sender;

impl<M: Memo> Optimizer<M> {
    /// Creates a new task to optimize a logical plan into a physical plan.
    ///
    /// This function only allocates a task ID and registes it, but does not yet
    /// launch the task until we know the corresponding goal that has to be optimized.
    ///
    /// # Parameters
    /// * `plan`: The logical plan to be optimized.
    /// * `physical_tx`: The channel to send the optimized physical plans back.
    pub(crate) fn create_optimize_plan_task(
        &mut self,
        plan: LogicalPlan,
        physical_tx: Sender<PhysicalPlan>,
    ) -> TaskId {
        use Task::*;

        let task_id = self.next_task_id();
        let optimize_plan_task = OptimizePlanTask {
            plan,
            physical_tx,
            optimize_goal_in: None,
        };
        self.add_task(task_id, OptimizePlan(optimize_plan_task));

        task_id
    }

    /// Launches the task to optimize a logical plan into a physical plan.
    ///
    /// This function assumes the goal_id is known and that the logical plan has been
    /// properly ingested.
    ///
    /// # Parameters
    /// * `task_id`: The ID of the task to be launched.
    /// * `goal_id`: The goal ID that this task is optimizing for.
    pub(crate) async fn launch_optimize_plan_task(
        &mut self,
        task_id: TaskId,
        goal_id: GoalId,
    ) -> Result<(), M::MemoError> {
        // Launch goal optimize task if needed, and get its ID.
        let goal_optimize_task_id = self.ensure_optimize_goal_task(goal_id).await?;

        // Register task and connect in graph.
        let optimize_plan_task = self.get_optimize_plan_task_mut(task_id).unwrap();
        optimize_plan_task.optimize_goal_in = Some(goal_optimize_task_id);

        // Add this task to the goal's outgoing edges.
        self.get_optimize_goal_task_mut(goal_optimize_task_id)
            .unwrap()
            .optimize_plan_out
            .insert(task_id);

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
    ) -> Result<(), M::MemoError> {
        use Task::*;

        let fork_task_id = self.next_task_id();

        // Launch the group exploration task if needed, and get its ID.
        let explore_group_in = self.ensure_group_exploration_task(group_id).await?;

        // Create continuation tasks for all expressions.
        let expressions = self
            .get_explore_group_task(explore_group_in)
            .unwrap()
            .dispatched_exprs
            .clone();
        let continue_with_logical_in =
            self.create_logical_cont_tasks(&expressions, group_id, fork_task_id, &continuation);

        // Create the fork task.
        let fork_logical_task = ForkLogicalTask {
            continuation,
            _out: parent_task_id,
            explore_group_in,
            continue_with_logical_in,
        };

        // Add the fork task to the exploration task's outgoing edges.
        self.get_explore_group_task_mut(explore_group_in)
            .unwrap()
            .fork_logical_out
            .insert(fork_task_id);

        self.add_task(fork_task_id, ForkLogical(fork_logical_task));

        Ok(())
    }

    /// Method to launch a task for continuing with a logical expression.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the logical expression to continue with.
    /// * `group_id`: The ID of the group that this expression belongs to.
    /// * `fork_out`: The ID of the fork task that this continue task feeds into.
    /// * `continuation`: The logical continuation to be used.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the created continue task.
    pub(crate) fn launch_continue_with_logical_task(
        &mut self,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        fork_out: TaskId,
        continuation: LogicalContinuation,
    ) -> TaskId {
        use Task::*;

        let task_id = self.next_task_id();
        let task = ContinueWithLogicalTask {
            expression_id,
            fork_out,
            fork_in: None,
        };

        self.add_task(task_id, ContinueWithLogical(task));
        self.schedule_job(
            task_id,
            JobKind::ContinueWithLogical(expression_id, group_id, continuation),
        );

        task_id
    }

    /// Method to launch a task for transforming a logical expression with a rule.
    ///
    /// # Parameters
    /// * `expr_id`: The ID of the logical expression to transform.
    /// * `rule`: The transformation rule to apply.
    /// * `explore_group_out`: The ID of the exploration task that this transformation feeds into.
    /// * `group_id`: The ID of the group that this transformation belongs to.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the created transform task.
    pub(crate) fn launch_transform_expression_task(
        &mut self,
        expr_id: LogicalExpressionId,
        rule: TransformationRule,
        explore_group_out: TaskId,
        group_id: GroupId,
    ) -> TaskId {
        use Task::*;

        let task_id = self.next_task_id();
        let task = TransformExpressionTask {
            _rule: rule.clone(),
            expression_id: expr_id,
            explore_group_out,
            fork_in: None,
        };

        self.add_task(task_id, TransformExpression(task));
        self.schedule_job(
            task_id,
            JobKind::TransformExpression(rule, expr_id, group_id),
        );

        task_id
    }

    /// Method to launch a task for implementing a logical expression with a rule.
    ///
    /// # Parameters
    /// * `expr_id`: The ID of the logical expression to implement.
    /// * `rule`: The implementation rule to apply.
    /// * `optimize_goal_out`: The ID of the optimization task that this implementation feeds into.
    /// * `goal_id`: The ID of the goal that this implementation belongs to.
    ///
    /// # Returns
    /// * `TaskId`: The ID of the created implement task.
    pub(crate) fn launch_implement_expression_task(
        &mut self,
        expr_id: LogicalExpressionId,
        rule: ImplementationRule,
        optimize_goal_out: TaskId,
        goal_id: GoalId,
    ) -> TaskId {
        use Task::*;

        let task_id = self.next_task_id();
        let task = ImplementExpressionTask {
            rule: rule.clone(),
            expression_id: expr_id,
            optimize_goal_out,
            fork_in: None,
        };

        self.add_task(task_id, ImplementExpression(task));
        self.schedule_job(
            task_id,
            JobKind::ImplementExpression(rule, expr_id, goal_id),
        );

        task_id
    }

    /// Creates transform tasks for a set of logical expressions.
    ///
    /// # Arguments
    /// * `expressions` - The logical expressions to transform
    /// * `group_id` - The group ID these expressions belong to
    /// * `explore_task_id` - The exploration task that these transforms feed into
    ///
    /// # Returns
    /// * `HashSet<TaskId>` - The IDs of all created transform tasks
    pub(crate) fn create_transform_tasks(
        &mut self,
        expressions: &HashSet<LogicalExpressionId>,
        group_id: GroupId,
        explore_task_id: TaskId,
    ) -> HashSet<TaskId> {
        let transformations = self.rule_book.get_transformations().to_vec();
        let mut transform_tasks = HashSet::new();

        for &expr_id in expressions {
            for rule in &transformations {
                let task_id = self.launch_transform_expression_task(
                    expr_id,
                    rule.clone(),
                    explore_task_id,
                    group_id,
                );
                transform_tasks.insert(task_id);
            }
        }

        transform_tasks
    }

    /// Creates implement tasks for a set of logical expressions.
    ///
    /// # Arguments
    /// * `expressions` - The logical expressions to implement
    /// * `goal_id` - The goal ID to implement for
    /// * `optimize_task_id` - The optimization task that these transforms feed into
    ///
    /// # Returns
    /// * `HashSet<TaskId>` - The IDs of all created implement tasks
    pub(crate) fn create_implement_tasks(
        &mut self,
        expressions: &HashSet<LogicalExpressionId>,
        goal_id: GoalId,
        optimize_task_id: TaskId,
    ) -> HashSet<TaskId> {
        let implementations = self.rule_book.get_implementations().to_vec();
        let mut implement_tasks = HashSet::new();

        for &expr_id in expressions {
            for rule in &implementations {
                let task_id = self.launch_implement_expression_task(
                    expr_id,
                    rule.clone(),
                    optimize_task_id,
                    goal_id,
                );
                implement_tasks.insert(task_id);
            }
        }

        implement_tasks
    }

    /// Creates logical continuation tasks for a fork task and a set of expressions.
    ///
    /// # Arguments
    /// * `expressions` - The logical expressions to continue with
    /// * `group_id` - The group ID these expressions belong to
    /// * `fork_task_id` - The fork task that these continuations feed into
    /// * `continuation` - The continuation to apply
    ///
    /// # Returns
    /// * `HashSet<TaskId>` - The IDs of all created continuation tasks
    pub(crate) fn create_logical_cont_tasks(
        &mut self,
        expressions: &HashSet<LogicalExpressionId>,
        group_id: GroupId,
        fork_task_id: TaskId,
        continuation: &LogicalContinuation,
    ) -> HashSet<TaskId> {
        let mut continuation_tasks = HashSet::new();
        for &expr_id in expressions {
            let task_id = self.launch_continue_with_logical_task(
                expr_id,
                group_id,
                fork_task_id,
                continuation.clone(),
            );
            continuation_tasks.insert(task_id);
        }

        continuation_tasks
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
    pub(crate) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
    ) -> Result<TaskId, M::MemoError> {
        use Task::*;

        // Find the representative group for the given group ID.
        let group_repr = self.memo.find_repr_group_id(group_id).await?;

        // Check if we already have an exploration task for this group.
        if let Some(task_id) = self.group_exploration_task_index.get(&group_repr) {
            return Ok(*task_id);
        }

        let exploration_task_id = self.next_task_id();

        // Create transform expression tasks for each logical expression and rule combination.
        let logical_expressions = self.memo.get_all_logical_exprs(group_repr).await?;
        let transform_expr_in =
            self.create_transform_tasks(&logical_expressions, group_id, exploration_task_id);

        // Create the exploration task.
        let exploration_task = ExploreGroupTask {
            group_id,
            dispatched_exprs: logical_expressions,
            optimize_goal_out: HashSet::new(),
            fork_logical_out: HashSet::new(),
            transform_expr_in,
        };

        // Register the task in the manager and index.
        self.add_task(exploration_task_id, ExploreGroup(exploration_task));
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
    #[async_recursion]
    pub(crate) async fn ensure_optimize_goal_task(
        &mut self,
        goal_id: GoalId,
    ) -> Result<TaskId, M::MemoError> {
        use Task::*;

        // Find the representative goal for the given goal ID.
        let goal_repr = self.memo.find_repr_goal_id(goal_id).await?;

        // Check if we already have an optimization task for this goal.
        if let Some(task_id) = self.goal_optimization_task_index.get(&goal_repr) {
            return Ok(*task_id);
        }

        let goal_optimize_task_id = self.next_task_id();

        let Goal(group_id, _) = self.memo.materialize_goal(goal_id).await?;
        let explore_group_in = self.ensure_group_exploration_task(group_id).await?;

        // Launch all implementation tasks.
        let expressions = self.memo.get_all_logical_exprs(group_id).await?;
        let implement_expression_in =
            self.create_implement_tasks(&expressions, goal_id, goal_optimize_task_id);

        let goal_optimize_task = OptimizeGoalTask {
            goal_id,
            optimize_plan_out: HashSet::new(),
            optimize_goal_out: HashSet::new(), // filled below to avoid infinite recursion
            optimize_goal_in: HashSet::new(),  // filled below to avoid infinite recursion
            explore_group_in,
            implement_expression_in,
        };

        // Add this task to the exploration task's outgoing edges.
        self.get_explore_group_task_mut(explore_group_in)
            .unwrap()
            .optimize_goal_out
            .insert(goal_optimize_task_id);

        // Register the task in the manager and index.
        self.add_task(goal_optimize_task_id, OptimizeGoal(goal_optimize_task));
        self.goal_optimization_task_index
            .insert(goal_repr, goal_optimize_task_id);

        // Ensure sub-goals are getting explored too, we do this after registering
        // the task to avoid infinite recursion.
        let sub_goals = self
            .memo
            .get_all_goal_members(goal_id)
            .await?
            .into_iter()
            .filter_map(|member_id| match member_id {
                GoalMemberId::GoalId(sub_goal_id) => Some(sub_goal_id),
                _ => None,
            });

        // Launch optimization tasks for each subgoal and establish links.
        let mut subgoal_task_ids = HashSet::new();
        for sub_goal_id in sub_goals {
            let sub_goal_task_id = self.ensure_optimize_goal_task(sub_goal_id).await?;
            subgoal_task_ids.insert(sub_goal_task_id);

            // Add current task to subgoal's outgoing edges.
            self.get_optimize_goal_task_mut(sub_goal_task_id)
                .unwrap()
                .optimize_goal_out
                .insert(goal_optimize_task_id);
        }

        // Update the parent task with links to subgoals.
        self.get_optimize_goal_task_mut(goal_optimize_task_id)
            .unwrap()
            .optimize_goal_in = subgoal_task_ids;

        Ok(goal_optimize_task_id)
    }
}
