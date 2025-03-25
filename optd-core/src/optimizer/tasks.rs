use super::{
    jobs::{JobId, JobKind},
    memo::{Memoize, Status},
    Optimizer,
};
use crate::{
    cir::{
        expressions::{LogicalExpressionId, PhysicalExpressionId},
        goal::{Goal, GoalId, GoalMemberId},
        group::GroupId,
        plans::{LogicalPlan, PhysicalPlan},
        properties::PhysicalProperties,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{CostedPhysicalPlanContinuation, LogicalPlanContinuation},
    error::Error,
};
use futures::channel::mpsc::Sender;
use std::collections::{HashMap, HashSet};
use JobKind::*;
use TaskKind::*;

//=============================================================================
// Type definitions and core structures
//=============================================================================

/// Unique identifier for tasks in the optimization system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TaskId(pub i64);

/// A task represents a higher-level objective in the optimization process.
///
/// Tasks are composed of one or more jobs and may depend on other tasks.
/// They represent structured, potentially hierarchical components of the
/// optimization process.
pub(super) struct Task {
    /// Tasks that created this task (parent tasks).
    pub parents: HashSet<TaskId>,

    /// Tasks that were created by this task (children tasks).
    pub children: HashSet<TaskId>,

    /// The specific kind of task.
    pub kind: TaskKind,

    /// Set of job IDs that must complete before this task is (temporarily) finished.
    pub uncompleted_jobs: HashSet<JobId>,
}

impl Task {
    /// Creates a new task with the specified kind and empty parent, child list, and job set.
    fn new(kind: TaskKind) -> Self {
        Self {
            parents: HashSet::new(),
            children: HashSet::new(),
            kind,
            uncompleted_jobs: HashSet::new(),
        }
    }
}

/// Enumeration of different types of tasks in the optimizer.
///
/// Each variant represents a structured component of the optimization process
/// that may launch multiple jobs and coordinate their execution.
pub(super) enum TaskKind {
    /// Top-level task to optimize a logical plan.
    OptimizePlan(OptimizePlanTask),

    /// Task to optimize a specific goal.
    OptimizeGoal(GoalId),

    /// Task to explore expressions in a logical group.
    ExploreGroup(GroupId),

    /// Task to apply a specific implementation rule to a logical expression.
    ImplementExpression(ImplementExpressionTask),

    /// Task to apply a specific transformation rule to a logical expression.
    TransformExpression(TransformExpressionTask),

    /// Task to compute the cost of a physical expression.
    CostExpression(CostExpressionTask),
}

//=============================================================================
// Task variant structs
//=============================================================================

/// Task data for optimizing a logical plan.
pub(super) struct OptimizePlanTask {
    /// The logical plan to be optimized.
    pub plan: LogicalPlan,

    /// Channel to send the optimized physical plan back to the caller.
    pub response_tx: Sender<PhysicalPlan>,
}

impl OptimizePlanTask {
    pub fn new(plan: LogicalPlan, response_tx: Sender<PhysicalPlan>) -> Self {
        Self { plan, response_tx }
    }
}

/// Task data for implementing a group with specific physical properties for a goal.
pub(super) struct ImplementGroupTask {
    /// The group to implement.
    pub group_id: GroupId,

    /// The physical properties to implement for.
    pub properties: PhysicalProperties,

    /// The goal ID this implementation is for.
    pub goal_id: GoalId,
}

impl ImplementGroupTask {
    pub fn new(group_id: GroupId, properties: PhysicalProperties, goal_id: GoalId) -> Self {
        Self {
            group_id,
            properties,
            goal_id,
        }
    }
}

/// Task data for implementing a logical expression using a specific rule.
pub(super) struct ImplementExpressionTask {
    /// The implementation rule to apply.
    pub rule: ImplementationRule,

    /// Whether the task has started the implementation rule.
    pub has_started: bool,

    /// The logical expression to implement.
    pub expression_id: LogicalExpressionId,

    /// The goal ID for this implementation.
    pub goal_id: GoalId,

    /// Continuations for each group that need to be notified when
    /// new logical expressions are created.
    pub continuations: HashMap<GroupId, Vec<LogicalPlanContinuation>>,
}

impl ImplementExpressionTask {
    pub fn new(
        rule: ImplementationRule,
        has_started: bool,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
    ) -> Self {
        Self {
            rule,
            has_started,
            expression_id,
            goal_id,
            continuations: HashMap::new(),
        }
    }
}

/// Task data for transforming a logical expression using a specific rule.
pub(super) struct TransformExpressionTask {
    /// The transformation rule to apply.
    pub rule: TransformationRule,

    /// Whether the task has started the transformation rule.
    pub has_started: bool,

    /// The logical expression to transform.
    pub expression_id: LogicalExpressionId,

    /// Continuations for each group that need to be notified when
    /// new logical expressions are created.
    pub continuations: HashMap<GroupId, Vec<LogicalPlanContinuation>>,
}

impl TransformExpressionTask {
    pub fn new(
        rule: TransformationRule,
        has_started: bool,
        expression_id: LogicalExpressionId,
    ) -> Self {
        Self {
            rule,
            has_started,
            expression_id,
            continuations: HashMap::new(),
        }
    }
}

/// Task data for costing a physical expression.
pub(super) struct CostExpressionTask {
    /// The physical expression to cost.
    pub expression_id: PhysicalExpressionId,

    /// Whether the task has started the cost estimation.
    pub has_started: bool,

    /// Continuations for each goal that need to be notified when
    /// optimized expressions are created.
    pub continuations: HashMap<GoalId, Vec<CostedPhysicalPlanContinuation>>,
}

impl CostExpressionTask {
    pub fn new(expression_id: PhysicalExpressionId, has_started: bool) -> Self {
        Self {
            expression_id,
            has_started,
            continuations: HashMap::new(),
        }
    }
}

//=============================================================================
// Optimizer task implementation
//=============================================================================

impl<M: Memoize> Optimizer<M> {
    //-------------------------------------------------------------------------
    // Public API
    //-------------------------------------------------------------------------

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
        let task = OptimizePlanTask::new(plan, response_tx);
        self.register_new_task(OptimizePlan(task), None)
    }

    /// Ensures a group exploration task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to explore all possible expressions in a group
    /// as part of its work. If an exploration task already exists, we reuse it.
    pub(super) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<(), Error> {
        let task_id = match self.group_exploration_task_index.get(&group_id) {
            Some(id) => *id,
            None => {
                self.launch_group_exploration_task(group_id, parent_task_id)
                    .await?
            }
        };

        self.register_parent_child_relationship(parent_task_id, task_id);
        Ok(())
    }

    /// Ensures a goal optimization task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to optimize a goal as part of its work.
    /// If an optimization task already exists, we reuse it.
    pub(super) async fn ensure_goal_optimize_task(
        &mut self,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<(), Error> {
        let task_id = match self.goal_optimization_task_index.get(&goal_id) {
            Some(id) => *id,
            None => {
                self.launch_goal_optimize_task(goal_id, parent_task_id)
                    .await?
            }
        };

        self.register_parent_child_relationship(parent_task_id, task_id);
        Ok(())
    }

    /// Ensures a cost expression task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to cost a physical expression as part of its work.
    /// If a costing task already exists, we reuse it.
    pub(super) async fn ensure_cost_expression_task(
        &mut self,
        expression_id: PhysicalExpressionId,
        parent_task_id: TaskId,
    ) -> Result<(), Error> {
        let task_id = match self.cost_expression_task_index.get(&expression_id) {
            Some(id) => *id,
            None => {
                let is_dirty = self.memo.get_cost_status(expression_id).await? == Status::Dirty;
                let task = CostExpressionTask::new(expression_id, is_dirty);
                let task_id = self.register_new_task(CostExpression(task), Some(parent_task_id));
                self.cost_expression_task_index
                    .insert(expression_id, task_id);

                if is_dirty {
                    self.schedule_job(task_id, StartCostExpression(expression_id));
                }

                task_id
            }
        };

        self.register_parent_child_relationship(parent_task_id, task_id);
        Ok(())
    }

    //-------------------------------------------------------------------------
    // Internal task launching methods
    //-------------------------------------------------------------------------

    /// Launches a task to start applying a transformation rule to a logical expression.
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    ///
    /// Only schedules the starting job if the transformation is marked as dirty in the memo.
    async fn launch_transform_expression_task(
        &mut self,
        rule: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        let is_dirty = self
            .memo
            .get_transformation_status(expression_id, &rule)
            .await?
            == Status::Dirty;

        let task = TransformExpressionTask::new(rule.clone(), is_dirty, expression_id);
        let task_id = self.register_new_task(TransformExpression(task), Some(parent_task_id));

        if is_dirty {
            self.schedule_job(
                task_id,
                StartTransformationRule(rule, expression_id, group_id),
            );
        }

        Ok(task_id)
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
    ) -> Result<TaskId, Error> {
        let is_dirty = self
            .memo
            .get_implementation_status(expression_id, goal_id, &rule)
            .await?
            == Status::Dirty;

        let task = ImplementExpressionTask::new(rule.clone(), is_dirty, expression_id, goal_id);
        let task_id = self.register_new_task(ImplementExpression(task), Some(parent_task_id));

        if is_dirty {
            self.schedule_job(
                task_id,
                StartImplementationRule(rule, expression_id, goal_id),
            );
        }

        Ok(task_id)
    }

    /// Launches a new task to explore all possible transformations for a logical group.
    ///
    /// This schedules jobs to apply all available transformation rules to all
    /// logical expressions in the group.
    async fn launch_group_exploration_task(
        &mut self,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.register_new_task(ExploreGroup(group_id), Some(parent_task_id));
        self.group_exploration_task_index.insert(group_id, task_id);

        // Launch the transformation task for all expression-rule combinations.
        let transformations = self.rule_book.get_transformations().to_vec();
        let expressions = self.memo.get_all_logical_exprs(group_id).await?;

        for expression_id in expressions {
            for rule in &transformations {
                self.launch_transform_expression_task(
                    rule.clone(),
                    expression_id,
                    group_id,
                    task_id,
                )
                .await?;
            }
        }

        Ok(task_id)
    }

    /// Launches a new task to optimize a goal.
    ///
    /// This method creates and manages the tasks needed to optimize a goal by
    /// ensuring group exploration, launching implementation tasks, and processing goal members.
    async fn launch_goal_optimize_task(
        &mut self,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.register_new_task(OptimizeGoal(goal_id), Some(parent_task_id));
        self.goal_optimization_task_index.insert(goal_id, task_id);

        // Launch implementation tasks for all expression-rule combinations.
        let Goal(group_id, _) = self.memo.materialize_goal(goal_id).await?;
        self.ensure_group_exploration_task(group_id, task_id)
            .await?;

        // Launch implementation tasks for all logical expressions in the group.
        let logical_expressions = self.memo.get_all_logical_exprs(group_id).await?;
        let implementations = self.rule_book.get_implementations().to_vec();

        for expr_id in logical_expressions {
            for rule in &implementations {
                self.launch_implement_expression_task(rule.clone(), expr_id, goal_id, task_id)
                    .await?;
            }
        }

        // Process all goal members: physical expressions and subgoals.
        let goal_members = self.memo.get_all_goal_members(goal_id).await?;
        for member in goal_members {
            match member {
                GoalMemberId::PhysicalExpressionId(expr_id) => {
                    self.ensure_cost_expression_task(expr_id, task_id).await?;
                }
                GoalMemberId::GoalId(referenced_goal_id) => {
                    self.ensure_goal_optimize_task(referenced_goal_id, task_id)
                        .await?;
                }
            }
        }

        Ok(task_id)
    }

    //-------------------------------------------------------------------------
    // Helper methods for task management
    //-------------------------------------------------------------------------

    /// Helper method to register a new task of a specified kind.
    ///
    /// Assigns a unique task ID, adds the task to the task registry,
    /// and sets up the parent relationship if provided.
    fn register_new_task(&mut self, kind: TaskKind, parent: Option<TaskId>) -> TaskId {
        // Generate a unique task ID.
        let task_id = self.next_task_id;
        self.next_task_id.0 += 1;

        // Create and register the task.
        let mut task = Task::new(kind);
        if let Some(parent_id) = parent {
            task.parents.insert(parent_id);
        }
        self.tasks.insert(task_id, task);

        task_id
    }

    /// Sets up the bidirectional parent-child relationship between tasks.
    ///
    /// Updates both the parent's children list and the child's parents set.
    fn register_parent_child_relationship(&mut self, parent_id: TaskId, child_id: TaskId) {
        // Add child to parent's children list if not already there.
        let parent = self.tasks.get_mut(&parent_id).unwrap();
        parent.children.insert(child_id);

        // Add parent to child's parents set.
        let child = self.tasks.get_mut(&child_id).unwrap();
        child.parents.insert(parent_id);
    }
}
