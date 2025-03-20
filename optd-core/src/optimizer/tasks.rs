use super::{
    jobs::{JobId, JobKind},
    memo::{Memoize, Status},
    Optimizer,
};
use crate::{
    cir::{
        expressions::{LogicalExpressionId, PhysicalExpressionId},
        goal::{Goal, GoalId},
        group::GroupId,
        plans::{LogicalPlan, PhysicalPlan},
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{CostedPhysicalPlanContrinuation, LogicalPlanContinuation},
    error::Error,
};
use futures::channel::mpsc::Sender;
use std::collections::{HashMap, HashSet};
use TaskKind::*;

//
// Type definitions
//

/// Unique identifier for tasks in the optimization system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TaskId(pub i64);

//
// Core task structures
//

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

impl Task {
    /// Creates a new task with the specified kind and empty child list and job set
    fn new(kind: TaskKind) -> Self {
        Self {
            children: Vec::new(),
            kind,
            uncompleted_jobs: HashSet::new(),
        }
    }
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
    OptimizePlan(OptimizePlanTask),

    /// Task to explore implementations for a specific goal
    ///
    /// This task generates and evaluates physical implementations that
    /// satisfy the properties required by the goal.
    ExploreGoal(ExploreGoalTask),

    /// Task to explore expressions in a logical group
    ///
    /// This task generates alternative logical expressions within an
    /// equivalence class through rule application.
    ExploreGroup(GroupId),

    /// Task to apply a specific implementation rule to a logical expression
    ///
    /// This task generates physical implementations from a logical expression
    /// using a specified implementation strategy. It maintains a set of continuations
    /// that will be notified of the implementation results.
    ImplementExpression(ImplementExpressionTask),

    /// Task to apply a specific transformation rule to a logical expression
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    TransformExpression(TransformExpressionTask),

    /// Task to compute the cost of a physical expression
    ///
    /// This task estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan. It maintains a set of continuations
    /// that will be notified of the costing results.
    CostExpression(CostExpressionTask),
}

//
// Task variant structs
//

/// Task data for optimizing a logical plan
///
/// Contains the original logical plan and a channel for returning
/// the optimized physical plan when optimization is complete.
pub(super) struct OptimizePlanTask {
    /// The logical plan to be optimized
    pub plan: LogicalPlan,

    /// Channel to send the optimized physical plan back to the caller
    pub response_tx: Sender<PhysicalPlan>,
}

impl OptimizePlanTask {
    /// Creates a new optimize plan task with the given plan and response channel
    pub fn new(plan: LogicalPlan, response_tx: Sender<PhysicalPlan>) -> Self {
        Self { plan, response_tx }
    }
}

/// Task data for exploring a goal and its equivalent goals
///
/// Contains a mapping of group IDs to their associated physical properties
/// for all goals in the equivalence class.
pub(super) struct ExploreGoalTask {
    /// All equivalent goals in the task
    pub equivalent_goals: Vec<GoalId>,
}

impl ExploreGoalTask {
    /// Creates a new goal exploration task
    pub fn new(equivalent_goals: Vec<GoalId>) -> Self {
        Self { equivalent_goals }
    }
}

/// Task data for implementing a logical expression using a specific rule
///
/// Tracks the implementation rule, expression, and continuation callbacks
/// for notifying dependent tasks about implementation results.
pub(super) struct ImplementExpressionTask {
    /// The implementation rule to apply
    pub rule: ImplementationRule,

    // Whether the task has launched the implementation rule:
    // this field remains false while the memo status is clean.
    pub is_launched: bool,

    /// The logical expression to implement
    pub expression_id: LogicalExpressionId,

    pub goal_id: GoalId,

    /// Continuations for each group that need to be notified when
    /// new logical expressions are created
    pub continuations: HashMap<GroupId, Vec<LogicalPlanContinuation>>,
}

impl ImplementExpressionTask {
    /// Creates a new implementation task with the given rule and expression
    pub fn new(
        rule: ImplementationRule,
        is_launched: bool,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
    ) -> Self {
        Self {
            rule,
            is_launched,
            expression_id,
            goal_id,
            continuations: HashMap::new(),
        }
    }

    /// Registers a logical expression continuation for a specific group
    ///
    /// Multiple continuations can be registered for the same group, and all will
    /// be notified when new logical expressions are created for that group.
    ///
    /// # Parameters
    /// * `group_id` - The group ID to associate this continuation with
    /// * `continuation` - The continuation to call when a new expression is created
    pub(super) fn register_continuation(
        &mut self,
        group_id: GroupId,
        continuation: LogicalPlanContinuation,
    ) {
        self.continuations
            .entry(group_id)
            .or_default()
            .push(continuation);
    }
}

/// Task data for transforming a logical expression using a specific rule
///
/// Tracks the transformation rule, expression, and continuation callbacks
/// for notifying dependent tasks about transformation results.
pub(super) struct TransformExpressionTask {
    /// The transformation rule to apply
    pub rule: TransformationRule,

    /// The logical expression to transform
    pub expression_id: LogicalExpressionId,

    /// Continuations for each group that need to be notified when
    /// new logical expressions are created
    pub continuations: HashMap<GroupId, Vec<LogicalPlanContinuation>>,
}

impl TransformExpressionTask {
    /// Creates a new transformation task with the given rule and expression
    pub fn new(rule: TransformationRule, expression_id: LogicalExpressionId) -> Self {
        Self {
            rule,
            expression_id,
            continuations: HashMap::new(),
        }
    }

    /// Registers a logical expression continuation for a specific group
    ///
    /// Multiple continuations can be registered for the same group, and all will
    /// be notified when new logical expressions are created for that group.
    ///
    /// # Parameters
    /// * `group_id` - The group ID to associate this continuation with
    /// * `continuation` - The continuation to call when a new expression is created
    pub(super) fn register_continuation(
        &mut self,
        group_id: GroupId,
        continuation: LogicalPlanContinuation,
    ) {
        self.continuations
            .entry(group_id)
            .or_default()
            .push(continuation);
    }
}

/// Task data for costing a physical expression
///
/// Tracks the physical expression and continuation callbacks for
/// notifying dependent tasks about costing results.
pub(super) struct CostExpressionTask {
    /// The physical expression to cost
    pub expression_id: PhysicalExpressionId,

    /// Continuations for each goal that need to be notified when
    /// optimized expressions are created
    pub continuations: HashMap<GoalId, Vec<CostedPhysicalPlanContrinuation>>,
}

impl CostExpressionTask {
    /// Creates a new cost estimation task for the given physical expression
    pub fn new(expression_id: PhysicalExpressionId) -> Self {
        Self {
            expression_id,
            continuations: HashMap::new(),
        }
    }

    /// Registers an optimized expression continuation for a specific goal
    ///
    /// Multiple continuations can be registered for the same goal, and all will
    /// be notified when new optimized expressions are created for that goal.
    ///
    /// # Parameters
    /// * `goal` - The goal to associate this continuation with
    /// * `continuation` - The continuation to call when a new optimized expression is created
    pub(super) fn register_continuation(
        &mut self,
        goal: GoalId,
        continuation: CostedPhysicalPlanContrinuation,
    ) {
        self.continuations
            .entry(goal)
            .or_default()
            .push(continuation);
    }
}

//
// Optimizer task implementation
//

impl<M: Memoize> Optimizer<M> {
    //
    // Task launching
    //

    pub(super) async fn launch_optimize_plan_task(
        &mut self,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) -> TaskId {
        let task_kind = OptimizePlanTask::new(plan, response_tx);
        self.register_new_task(OptimizePlan(task_kind))
    }

    /// Launches a task to start applying a transformation rule to a logical expression
    ///
    /// Transformation rules create alternative logical expressions that are semantically
    /// equivalent to the original, expanding the search space.
    pub(super) fn launch_transform_expression_task(
        &mut self,
        rule: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        parent: TaskId,
    ) -> TaskId {
        // Create and register the transformation task
        let task_kind = TransformExpressionTask::new(rule.clone(), expression_id);
        let task_id = self.register_new_task(TransformExpression(task_kind));

        // Register this task as a child of the parent task
        self.register_child_to_task(parent, task_id);

        // Schedule a job to actually launch the transformation rule
        self.schedule_job(
            task_id,
            JobKind::transformation_rule(rule, expression_id, group_id),
        );

        task_id
    }

    /// Launches a task to start applying an implementation rule to a logical expression
    /// and physical properties to create physical expressions
    ///
    /// Implementation rules convert logical expressions and properties to equivalent
    /// physical expressions which represent concrete execution strategies.
    pub(super) async fn launch_implement_expression_task(
        &mut self,
        rule: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        parent: TaskId,
    ) -> TaskId {
        let is_dirty = self
            .memo
            .get_implementation_status(expression_id, goal_id, &rule)
            .await
            .expect("Failed to get implementation status")
            == Status::Dirty;

        let task_kind =
            ImplementExpressionTask::new(rule.clone(), is_dirty, expression_id, goal_id);
        let task_id = self.register_new_task(ImplementExpression(task_kind));
        self.register_child_to_task(parent, task_id);

        if is_dirty {
            self.schedule_job(
                task_id,
                JobKind::implementation_rule(rule, expression_id, goal_id),
            );
        }

        task_id
    }

    /// Launches a task to start computing the cost of a physical expression
    ///
    /// Cost estimation helps determine which physical implementation is best
    /// among the alternatives.
    pub(super) fn launch_cost_expression_task(
        &mut self,
        expression_id: PhysicalExpressionId,
        parent: TaskId,
    ) -> TaskId {
        // Create and register the cost estimation task
        let task_kind = CostExpressionTask::new(expression_id);
        let task_id = self.register_new_task(CostExpression(task_kind));

        // Set up parent-child relationship
        self.register_child_to_task(parent, task_id);

        // Schedule a job to actually launch the cost estimation
        self.schedule_job(task_id, JobKind::cost_expression(expression_id));

        task_id
    }

    //
    // Exploration tasks
    //

    /// Ensures a group exploration task exists and sets up a parent-child relationship
    ///
    /// This is used when a task needs to explore all possible expressions in a group
    /// as part of its work. If an exploration task already exists, we reuse it.
    pub(super) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
        child_task_id: TaskId,
    ) -> Result<(), Error> {
        // Check if we already have an exploration task for this group
        let task_id = match self.group_explorations_task_index.get(&group_id) {
            Some(id) => *id,
            None => self.launch_group_exploration_task(group_id).await,
        };

        // Set up parent-child relationship
        self.register_child_to_task(task_id, child_task_id);

        Ok(())
    }

    /// Ensures a goal exploration task exists and sets up a parent-child relationship
    ///
    /// This is used when a task needs to explore all possible implementations for a goal
    /// as part of its work. If an exploration task already exists, we reuse it.
    pub(super) async fn ensure_goal_exploration_task(
        &mut self,
        goal_id: GoalId,
        child_task_id: TaskId,
    ) -> Result<(), Error> {
        // Check if we already have an exploration task for this goal
        let task_id = match self.goal_exploration_task_index.get(&goal_id) {
            Some(id) => *id,
            None => self.launch_goal_exploration_task(goal_id).await?,
        };

        // Set up parent-child relationship
        self.register_child_to_task(task_id, child_task_id);

        Ok(())
    }

    //
    // Helper methods for launching exploration tasks
    //

    /// Launches a new task to explore all possible transformations for a logical group
    ///
    /// This schedules jobs to apply all available transformation rules to all
    /// logical expressions in the group.
    async fn launch_group_exploration_task(&mut self, group_id: GroupId) -> TaskId {
        // Create task and register it in the group exploration index
        let task_id = self.register_new_task(ExploreGroup(group_id));
        self.group_explorations_task_index.insert(group_id, task_id);

        // Get all transformation rules and all logical expressions in the group
        let transformations = self.rule_book.get_transformations().to_vec();
        let expressions = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical expressions for group");

        // Schedule transformation tasks for all expression-rule combinations
        // This creates a Cartesian product of rules × expressions
        expressions
            .into_iter()
            .flat_map(|expr_id| {
                transformations
                    .iter()
                    .map(move |rule| (rule.clone(), expr_id))
            })
            .for_each(|(rule, expr_id)| {
                self.launch_transform_expression_task(rule, expr_id, group_id, task_id);
            });

        task_id
    }

    /// Launches a new task to explore all possible implementations for a goal
    ///
    /// This schedules jobs to apply all implementation rules to all logical expressions
    /// and launches cost tasks for all physical expressions in all equivalent goals.
    /// It handles multiple equivalent goals across different groups with various physical properties.
    async fn launch_goal_exploration_task(&mut self, goal_id: GoalId) -> Result<TaskId, Error> {
        // Get all equivalent goals grouped by group ID and their associated physical properties
        let equivalent_goals = self.memo.get_equivalent_goals(goal_id).await?;

        // Create task and register it in the goal exploration index
        let task_kind = ExploreGoalTask::new(equivalent_goals.clone());
        let task_id = self.register_new_task(ExploreGoal(task_kind));
        self.goal_exploration_task_index.insert(goal_id, task_id);

        for goal_id in equivalent_goals {
            let Goal(group_id, _) = self.memo.materialize_goal(goal_id).await?;

            // Ensure we explore each group associated with the equivalent goals
            // This is necessary since new logical expressions can lead to new implementations
            self.ensure_group_exploration_task(group_id, task_id).await;

            let implementations = self.rule_book.get_implementations().to_vec();
            let logical_expressions = self
                .memo
                .get_all_logical_exprs(group_id)
                .await
                .expect("Failed to get logical expressions for group");

            // Schedule implementation jobs for all expression-rule-properties combinations for this group
            // This creates a Cartesian product of rules × properties × expressions.
            logical_expressions
                .into_iter()
                .flat_map(|expr| {
                    implementations
                        .iter()
                        .map(move |rule| (rule.clone(), expr.clone()))
                })
                .for_each(|(rule, expr_id)| {
                    self.launch_implement_expression_task(rule, expr_id, goal_id, task_id);
                });
        }

        // Launch costing for all physical expressions in the original goal
        // (this includes all the equivalent goals).
        let physical_expressions = self
            .memo
            .get_all_physical_exprs(goal_id)
            .await
            .expect("Failed to get physical expressions for goal");

        physical_expressions.into_iter().for_each(|expr_id| {
            self.launch_cost_expression_task(expr_id, task_id);
        });

        Ok(task_id)
    }

    /// Helper method to register a new task of a specified kind
    ///
    /// Assigns a unique task ID and adds the task to the task registry.
    fn register_new_task(&mut self, kind: TaskKind) -> TaskId {
        // Generate a unique task ID
        let task_id = self.next_task_id;
        self.next_task_id.0 += 1;

        // Create and register the task
        self.tasks.insert(task_id, Task::new(kind));
        task_id
    }

    /// Helper method to register a child task to a parent task
    ///
    /// This sets up the hierarchical relationship between tasks.
    fn register_child_to_task(&mut self, parent_id: TaskId, child_id: TaskId) {
        self.tasks
            .get_mut(&parent_id)
            .expect("Parent task not found")
            .children
            .push(child_id);
    }
}
