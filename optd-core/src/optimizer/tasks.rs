use super::{jobs::JobId, memo::Memoize, Optimizer};
use crate::{
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        plans::LogicalPlan,
        rules::{ImplementationRule, TransformationRule},
    },
    engine::{LogicalExprContinuation, OptimizedExprContinuation},
};
use std::collections::{HashMap, HashSet};

/// Unique identifier for tasks in the optimization system
pub(super) type TaskId = i64;

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

/// Subscribers for a group in a transformation task
pub(super) struct TransformSubscribers {
    /// Map of group IDs to their continuations
    pub subscribers: HashMap<GroupId, Vec<LogicalExprContinuation>>,
}

/// Subscribers for a goal in an implementation task
pub(super) struct ImplementSubscribers {
    /// Map of group IDs to their continuations
    pub subscribers: HashMap<GroupId, Vec<LogicalExprContinuation>>,
}

/// Subscribers for a goal in a cost evaluation task
pub(super) struct CostSubscribers {
    /// Map of goals to their continuations
    pub subscribers: HashMap<Goal, Vec<OptimizedExprContinuation>>,
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
    OptimizePlan(LogicalPlan),

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
    /// using a specified implementation strategy. It maintains a set of subscribers
    /// that will be notified of the implementation results.
    ImplementExpression(ImplementationRule, LogicalExpression, ImplementSubscribers),

    /// Task to apply a specific transformation rule to a logical expression
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of subscribers
    /// that will be notified of the transformation results.
    TransformExpression(TransformationRule, LogicalExpression, TransformSubscribers),

    /// Task to compute the cost of a physical expression
    ///
    /// This task estimates the execution cost of a physical implementation
    /// to aid in selecting the optimal plan. It maintains a set of subscribers
    /// that will be notified of the costing results.
    CostExpression(PhysicalExpression, CostSubscribers),
}

/// Task-related implementation for the Optimizer
impl<M: Memoize> Optimizer<M> {
    /// Ensures a group exploration task exists and sets up a parent-child relationship
    ///
    /// This method finds an existing exploration task for a group or creates one
    /// if none exists. It then establishes a parent-child relationship between the
    /// exploration task and the subscriber task.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group to explore
    /// * `child_task_id` - The ID of the subscriber task
    pub(super) async fn ensure_group_exploration_task(
        &mut self,
        group_id: GroupId,
        child_task_id: TaskId,
    ) {
        // Find or create the exploration task
        let task_id = match self.find_group_exploration_task(group_id) {
            Some(id) => id,
            None => self.create_group_exploration_task(group_id).await,
        };

        // Add the subscriber task as a child of the exploration task
        self.add_child_to_task(task_id, child_task_id);
    }

    /// Ensures a goal exploration task exists and sets up a parent-child relationship
    ///
    /// This method finds an existing exploration task for a goal or creates one
    /// if none exists. It then establishes a parent-child relationship between the
    /// exploration task and the subscriber task.
    ///
    /// # Parameters
    /// * `goal` - The goal to explore
    /// * `child_task_id` - The ID of the subscriber task
    pub(super) async fn ensure_goal_exploration_task(&mut self, goal: Goal, child_task_id: TaskId) {
        // Find or create the exploration task
        let task_id = match self.find_goal_exploration_task(&goal) {
            Some(id) => id,
            None => self.create_goal_exploration_task(goal.clone()).await,
        };

        // Add the subscriber task as a child of the exploration task
        self.add_child_to_task(task_id, child_task_id);
    }

    /// Creates a group exploration task with bootstrapped jobs
    async fn create_group_exploration_task(&mut self, group_id: GroupId) -> TaskId {
        // Create the exploration task
        let task_id = self.create_task(TaskKind::ExploreGroup(group_id));

        // Schedule transformation jobs for existing expressions
        let expressions = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical expressions for group");

        self.create_transformation_jobs(task_id, &expressions);

        task_id
    }

    /// Creates a goal exploration task with bootstrapped jobs
    async fn create_goal_exploration_task(&mut self, goal: Goal) -> TaskId {
        // Create the exploration task
        let task_id = self.create_task(TaskKind::ExploreGoal(goal.clone()));

        // Schedule implementation jobs for existing expressions
        let expressions = self
            .memo
            .get_all_logical_exprs(goal.0)
            .await
            .expect("Failed to get logical expressions for goal");

        self.create_implementation_jobs(task_id, &expressions);

        // Schedule cost jobs for existing physical expressions
        let physical_exprs = self
            .memo
            .get_all_physical_exprs(&goal)
            .await
            .expect("Failed to get physical expressions for goal");

        self.create_cost_jobs(task_id, &physical_exprs);

        // Ensure we also explore the group associated with this goal
        self.ensure_group_exploration_task(goal.0, task_id).await;

        task_id
    }

    /// Finds an existing exploration task for a group
    fn find_group_exploration_task(&self, group_id: GroupId) -> Option<TaskId> {
        self.tasks.iter().find_map(|(id, task)| {
            if let TaskKind::ExploreGroup(task_group_id) = &task.kind {
                if *task_group_id == group_id {
                    return Some(*id);
                }
            }
            None
        })
    }

    /// Finds an existing exploration task for a goal
    fn find_goal_exploration_task(&self, goal: &Goal) -> Option<TaskId> {
        self.tasks.iter().find_map(|(id, task)| {
            if let TaskKind::ExploreGoal(task_goal) = &task.kind {
                if *task_goal == *goal {
                    return Some(*id);
                }
            }
            None
        })
    }

    /// Creates a new task with the specified kind
    fn create_task(&mut self, kind: TaskKind) -> TaskId {
        let task_id = self.next_task_id;
        self.next_task_id += 1;

        let task = Task {
            children: Vec::new(),
            kind,
            uncompleted_jobs: HashSet::new(),
        };

        self.tasks.insert(task_id, task);
        task_id
    }

    /// Adds a child task to a parent task if not already present
    fn add_child_to_task(&mut self, parent_id: TaskId, child_id: TaskId) {
        if let Some(task) = self.tasks.get_mut(&parent_id) {
            if !task.children.contains(&child_id) {
                task.children.push(child_id);
            }
        }
    }
}
