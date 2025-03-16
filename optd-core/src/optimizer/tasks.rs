use super::{
    ingest::LogicalIngest,
    jobs::{JobId, JobKind},
    memo::Memoize,
    Optimizer,
};
use crate::{
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PhysicalPlan},
        properties::PhysicalProperties,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{LogicalExprContinuation, OptimizedExprContinuation},
};
use futures::channel::mpsc::Sender;
use std::collections::{HashMap, HashSet};
use JobKind::*;
use TaskKind::*;

//
// Type definitions
//

/// Unique identifier for tasks in the optimization system
pub(super) type TaskId = i64;

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
    ExploreGoal(Goal),

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

pub(super) struct OptimizePlanTask {
    pub plan: LogicalPlan,
    pub response_tx: Sender<PhysicalPlan>,
}

impl OptimizePlanTask {
    pub fn new(plan: LogicalPlan, response_tx: Sender<PhysicalPlan>) -> Self {
        Self { plan, response_tx }
    }
}

pub(super) struct ImplementExpressionTask {
    pub rule: ImplementationRule,
    pub expr: LogicalExpression,
    pub conts: HashMap<GroupId, Vec<LogicalExprContinuation>>,
}

impl ImplementExpressionTask {
    pub fn new(rule: ImplementationRule, expr: LogicalExpression) -> Self {
        Self {
            rule,
            expr,
            conts: HashMap::new(),
        }
    }
}

pub(super) struct TransformExpressionTask {
    pub rule: TransformationRule,
    pub expr: LogicalExpression,
    pub conts: HashMap<GroupId, Vec<LogicalExprContinuation>>,
}

impl TransformExpressionTask {
    pub fn new(rule: TransformationRule, expr: LogicalExpression) -> Self {
        Self {
            rule,
            expr,
            conts: HashMap::new(),
        }
    }
}

pub(super) struct CostExpressionTask {
    pub expr: PhysicalExpression,
    pub conts: HashMap<Goal, Vec<OptimizedExprContinuation>>,
}

impl CostExpressionTask {
    pub fn new(expr: PhysicalExpression) -> Self {
        Self {
            expr,
            conts: HashMap::new(),
        }
    }
}

/// Result of attempting to launch an optimize plan task
pub(super) enum OptimizePlanResult {
    Success(TaskId),
    NeedsDependencies(HashSet<JobId>),
}

//
// Optimizer task implementation
//

impl<M: Memoize> Optimizer<M> {
    //
    // Task launching
    //

    /// Tries to launch a top-level optimization task for a logical plan
    pub(super) async fn launch_optimize_plan_task(
        &mut self,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) -> OptimizePlanResult {
        // First try to ingest the plan into the memo
        match self.try_ingest_logical(&plan.clone().into(), 0).await {
            LogicalIngest::Success(group_id) => {
                let task_kind = OptimizePlanTask::new(plan, response_tx);
                let task_id = self.add_new_task(OptimizePlan(task_kind));

                // Create a goal for the root group
                let goal = Goal(group_id, PhysicalProperties(None));
                let goal = self.goal_repr.find(&goal);

                // Subscribe the task to the goal
                self.subscribe_task_to_goal(goal, task_id).await;

                OptimizePlanResult::Success(task_id)
            }
            LogicalIngest::NeedsDependencies(dependencies) => {
                // Cannot launch the task yet, need to wait for dependencies
                OptimizePlanResult::NeedsDependencies(dependencies)
            }
        }
    }

    /// Launches a task to start applying a transformation rule to a logical expression
    pub(super) fn launch_transform_expression_task(
        &mut self,
        rule: TransformationRule,
        expr: LogicalExpression,
        parent: TaskId,
    ) -> TaskId {
        let task_kind = TransformExpressionTask::new(rule.clone(), expr.clone());
        let task_id = self.add_new_task(TransformExpression(task_kind));

        self.add_child_to_task(parent, task_id);
        self.schedule_job(task_id, LaunchTransformationRule(rule, expr));

        task_id
    }

    /// Launches a task to start applying an implementation rule to a logical expression
    pub(super) fn launch_implement_expression_task(
        &mut self,
        rule: ImplementationRule,
        expr: LogicalExpression,
        parent: TaskId,
    ) -> TaskId {
        let task_kind = ImplementExpressionTask::new(rule.clone(), expr.clone());
        let task_id = self.add_new_task(ImplementExpression(task_kind));

        self.add_child_to_task(parent, task_id);
        self.schedule_job(task_id, LaunchImplementationRule(rule, expr));

        task_id
    }

    /// Launches a task to start computing the cost of a physical expression
    pub(super) fn lauch_cost_expression_task(
        &mut self,
        expr: PhysicalExpression,
        parent: TaskId,
    ) -> TaskId {
        let task_kind = CostExpressionTask::new(expr.clone());
        let task_id = self.add_new_task(CostExpression(task_kind));

        self.add_child_to_task(parent, task_id);
        self.schedule_job(task_id, LaunchCostExpression(expr));

        task_id
    }

    //
    // Exploration tasks
    //

    /// Ensures a group exploration task exists and sets up a parent-child relationship
    pub(super) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
        child_task_id: TaskId,
    ) {
        let task_id = match self.group_explorations_task_index.get(&group_id) {
            Some(id) => *id,
            None => self.launch_group_exploration_task(group_id).await,
        };

        self.add_child_to_task(task_id, child_task_id);
    }

    /// Ensures a goal exploration task exists and sets up a parent-child relationship
    pub(super) async fn ensure_goal_exploration_task(
        &mut self,
        goal: &Goal,
        child_task_id: TaskId,
    ) {
        let task_id = match self.goal_exploration_task_index.get(&goal) {
            Some(id) => *id,
            None => self.launch_goal_exploration_task(goal).await,
        };

        self.add_child_to_task(task_id, child_task_id);
    }

    //
    // Helper methods for launching exploration tasks
    //

    async fn launch_group_exploration_task(&mut self, group_id: GroupId) -> TaskId {
        // Create task and register it in the group exploration index
        let task_id = self.add_new_task(ExploreGroup(group_id));
        self.group_explorations_task_index.insert(group_id, task_id);

        let transformations = self.rule_book.get_transformations().to_vec();
        let expressions = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical expressions for group");

        // Schedule transformation jobs for all expression-rule combinations
        expressions
            .into_iter()
            .flat_map(|expr| {
                transformations
                    .iter()
                    .map(move |rule| (rule.clone(), expr.clone()))
            })
            .for_each(|(rule, expr)| {
                self.schedule_job(task_id, LaunchTransformationRule(rule, expr));
            });

        task_id
    }

    async fn launch_goal_exploration_task(&mut self, goal: &Goal) -> TaskId {
        // Create task and register it in the goal exploration index
        let task_id = self.add_new_task(ExploreGoal(goal.clone()));
        self.goal_exploration_task_index
            .insert(goal.clone(), task_id);

        // Ensure we also explore the group associated with this goal
        self.ensure_group_exploration_task(goal.0, task_id).await;

        let implementations = self.rule_book.get_implementations().to_vec();
        let logical_expressions = self
            .memo
            .get_all_logical_exprs(goal.0)
            .await
            .expect("Failed to get logical expressions for goal");

        // Schedule implementation jobs for all expression-rule combinations
        logical_expressions
            .into_iter()
            .flat_map(|expr| {
                implementations
                    .iter()
                    .map(move |rule| (rule.clone(), expr.clone()))
            })
            .for_each(|(rule, expr)| {
                self.schedule_job(task_id, LaunchImplementationRule(rule, expr));
            });

        let physical_expressions = self
            .memo
            .get_all_physical_exprs(goal)
            .await
            .expect("Failed to get physical expression for goal");

        // Schedule costing jobs for all physical expressions
        physical_expressions.into_iter().for_each(|expr| {
            self.schedule_job(task_id, LaunchCostExpression(expr));
        });

        task_id
    }

    /// Helper method to add and allocate a new task with a specified kind
    fn add_new_task(&mut self, kind: TaskKind) -> TaskId {
        let task_id = self.next_task_id;
        self.next_task_id += 1;

        self.tasks.insert(task_id, Task::new(kind));
        task_id
    }

    /// Helper method to add a child task to a parent task if not already present
    fn add_child_to_task(&mut self, parent_id: TaskId, child_id: TaskId) {
        if let Some(task) = self.tasks.get_mut(&parent_id) {
            if !task.children.contains(&child_id) {
                task.children.push(child_id);
            }
        }
    }
}
