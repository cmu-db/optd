#![allow(dead_code)]

use super::{
    EngineMessageKind, Optimizer,
    jobs::{JobId, JobKind},
};
use crate::{
    cir::{
        Cost, Goal, GoalId, GoalMemberId, GroupId, ImplementationRule, LogicalExpressionId,
        LogicalPlan, PhysicalExpressionId, PhysicalPlan, TransformationRule,
    },
    error::Error,
    memo::{Memoize, Status},
};
use JobKind::*;
use futures::channel::mpsc::Sender;
use optd_dsl::{
    analyzer::hir::Value,
    engine::{Continuation, EngineResponse},
};
use std::collections::{HashMap, HashSet};

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
pub(super) enum Task {
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

/// Task data for optimizing a specific goal.
pub(super) struct OptimizeGoalTask {
    /// The goal to optimize.
    pub goal_id: GoalId,
    pub optimize_plan_out: Vec<TaskId>,
    pub optimize_goal_out: Vec<TaskId>,
    pub continue_with_out: Vec<TaskId>,
    pub optimize_goal_in: Vec<TaskId>,
    pub explore_group_in: TaskId,
    pub implement_expression_in: Vec<TaskId>,
    pub cost_expression_in: Vec<TaskId>,
}

/// Task data for exploring a group.
pub(super) struct ExploreGroupTask {
    /// The group to explore.
    pub group_id: GroupId,
    pub optimize_goal_out: Vec<TaskId>,
    pub continue_with_out: Vec<TaskId>,
    pub transform_expr_in: Vec<TaskId>,
}

pub(super) struct ForkLogicalTask {
    pub continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,

    /// ContinueWithLogical | TransformExpression | ImplementExpression
    /// (memo alexis: I don't think these matter too much for continuations...)
    pub out: TaskId,
    pub explore_group_in: TaskId,
    pub continue_tasks: Vec<TaskId>,
}

impl ForkLogicalTask {
    pub fn new(
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        out: TaskId,
        explore_group_in: TaskId,
    ) -> Self {
        Self {
            continuation,
            out,
            explore_group_in,
            continue_tasks: Vec::new(),
        }
    }
}

pub(super) struct ContinueWithLogicalTask {
    pub expr_id: LogicalExpressionId,
    pub fork_out: TaskId,
    pub fork_in: Option<TaskId>,
}

impl ContinueWithLogicalTask {
    pub fn new(expr_id: LogicalExpressionId, fork_out: TaskId) -> Self {
        Self {
            expr_id,
            fork_out,
            fork_in: None,
        }
    }
}

/// Task data for transforming a logical expression using a specific rule.
pub(super) struct TransformExpressionTask {
    /// The transformation rule to apply.
    pub rule: TransformationRule,

    /// Whether the task has started the transformation rule.
    pub is_processing: bool,

    /// The logical expression to transform.
    pub expression_id: LogicalExpressionId,

    pub explore_group_out: TaskId,

    pub fork_in: Option<TaskId>,
    /// Continuations for each group that need to be notified when
    /// new logical expressions are created.
    pub continuations:
        HashMap<GroupId, Vec<Continuation<Value, EngineResponse<EngineMessageKind>>>>,
}

impl TransformExpressionTask {
    pub fn new(
        rule: TransformationRule,
        is_processing: bool,
        expression_id: LogicalExpressionId,
    ) -> Self {
        Self {
            rule,
            is_processing,
            expression_id,
            fork_in: None,
            explore_group_out: TaskId(-1),
            continuations: HashMap::new(),
        }
    }
}

/// Task data for implementing a logical expression using a specific rule.
pub(super) struct ImplementExpressionTask {
    /// The implementation rule to apply.
    pub rule: ImplementationRule,

    /// Whether the task has started the implementation rule.
    pub is_processing: bool,

    /// The logical expression to implement.
    pub expression_id: LogicalExpressionId,

    pub optimize_goal_out: TaskId,

    pub goal_id: GoalId,

    pub fork_in: Option<TaskId>,

    /// Continuations for each group that need to be notified when
    /// new logical expressions are created.
    pub continuations:
        HashMap<GroupId, Vec<Continuation<Value, EngineResponse<EngineMessageKind>>>>,
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
            is_processing: has_started,
            expression_id,
            goal_id,
            optimize_goal_out: TaskId(-1),
            continuations: HashMap::new(),
            fork_in: None,
        }
    }
}

/// Task data for costing a physical expression.
pub(super) struct CostExpressionTask {
    /// The physical expression to cost.
    pub expr_id: PhysicalExpressionId,

    /// Whether the task has started the cost estimation.
    pub has_started: bool,

    pub budget: Cost,

    pub optimize_goal_out: Vec<TaskId>,

    pub fork_in: Option<TaskId>,

    /// Continuations for each goal that need to be notified when
    /// optimized expressions are created.
    pub continuations: HashMap<GoalId, Vec<Continuation<Value, EngineResponse<EngineMessageKind>>>>,
}

impl CostExpressionTask {
    pub fn new(expr_id: PhysicalExpressionId, has_started: bool) -> Self {
        Self {
            expr_id,
            has_started,
            continuations: HashMap::new(),
            budget: Cost(f64::MAX),
            optimize_goal_out: Vec::new(),
            fork_in: None,
        }
    }
}

pub(super) struct ForkCostedTask {
    pub continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,

    /// The current upper bound on the allowed cost budget.
    pub budget: Cost,

    /// ContinueWithCosted | CostExpresion
    /// (memo alexis: I don't think these matter too much for continuations...)
    pub out: TaskId,

    pub optimize_goal_in: TaskId,

    pub fork_in: Vec<TaskId>,
}

impl ForkCostedTask {
    pub fn new(
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        budget: Cost,
        out: TaskId,
        optimize_goal_in: TaskId,
    ) -> Self {
        Self {
            continuation,
            budget,
            out,
            optimize_goal_in,
            fork_in: Vec::new(),
        }
    }
}

pub(super) struct ContinueWithCostedTask {
    pub expr_id: PhysicalExpressionId,
    pub fork_out: TaskId,
    pub fork_in: Option<TaskId>,
}

impl ContinueWithCostedTask {
    pub fn new(expr_id: PhysicalExpressionId, fork_out: TaskId) -> Self {
        Self {
            expr_id,
            fork_out,
            fork_in: None,
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

    /// Create a new task to optimize a logical plan into a physical plan.
    ///
    /// This method creates and registers a task that will optimize the provided logical
    /// plan into a physical plan. The optimized plan will be sent back through the provided
    /// response channel every time a better plan is found.
    pub(super) async fn create_optimize_plan_task(
        &mut self,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) -> TaskId {
        // let task = OptimizePlanTask::new(plan, response_tx);
        // self.register_new_task(OptimizePlan(task), None)
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
    ) -> Result<TaskId, Error> {
        // let task_id = match self.group_exploration_task_index.get(&group_id) {
        //     Some(id) => *id,
        //     None => {
        //         self.create_group_exploration_task(group_id, parent_task_id)
        //             .await?
        //     }
        // };

        // // self.register_parent_child_relationship(parent_task_id, task_id);
        // // Ok(task_id)
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
    ) -> Result<(), Error> {
        // let task_id = match self.goal_optimization_task_index.get(&goal_id) {
        //     Some(id) => *id,
        //     None => {
        //         self.create_goal_optimize_task(goal_id, parent_task_id)
        //             .await?
        //     }
        // };

        // self.register_parent_child_relationship(parent_task_id, task_id);
        // Ok(())
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
    ) -> Result<(), Error> {
        // let task_id = match self.cost_expression_task_index.get(&expression_id) {
        //     Some(id) => *id,
        //     None => {
        //         let is_dirty = self.memo.get_cost_status(expression_id).await? == Status::Dirty;
        //         let task = CostExpressionTask::new(expression_id, is_dirty);
        //         let task_id = self.register_new_task(CostExpression(task), Some(parent_task_id));
        //         self.cost_expression_task_index
        //             .insert(expression_id, task_id);

        //         if is_dirty {
        //             self.schedule_job(task_id, StartCostExpression(expression_id));
        //         }

        //         task_id
        //     }
        // };

        // self.register_parent_child_relationship(parent_task_id, task_id);
        // Ok(())
        todo!()
    }

    //-------------------------------------------------------------------------
    // Internal task creation methods
    //-------------------------------------------------------------------------

    /// Creates a task to start applying a transformation rule to a logical expression.
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    ///
    /// Only schedules the starting job if the transformation is marked as dirty in the memo.
    async fn create_transform_expression_task(
        &mut self,
        rule: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        // let is_dirty = self
        //     .memo
        //     .get_transformation_status(expression_id, &rule)
        //     .await?
        //     == Status::Dirty;

        // let task = TransformExpressionTask::new(rule.clone(), is_dirty, expression_id);
        // let task_id = self.register_new_task(TransformExpression(task), Some(parent_task_id));

        // if is_dirty {
        //     self.schedule_job(
        //         task_id,
        //         StartTransformationRule(rule, expression_id, group_id),
        //     );
        // }
        // Ok(task_id)
        todo!()
    }

    /// Creates a task to start applying an implementation rule to a logical expression.
    ///
    /// This task generates physical implementations from a logical expression
    /// using a specified implementation strategy. It maintains a set of continuations
    /// that will be notified of the implementation results.
    ///
    /// Only schedules the starting job if the implementation is marked as dirty in the memo.
    async fn create_implement_expression_task(
        &mut self,
        rule: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        // let is_dirty = self
        //     .memo
        //     .get_implementation_status(expression_id, goal_id, &rule)
        //     .await?
        //     == Status::Dirty;

        // let task = ImplementExpressionTask::new(rule.clone(), is_dirty, expression_id, goal_id);
        // let task_id = self.register_new_task(ImplementExpression(task), Some(parent_task_id));

        // if is_dirty {
        //     self.schedule_job(
        //         task_id,
        //         StartImplementationRule(rule, expression_id, goal_id),
        //     );
        // }

        // Ok(task_id)
        todo!()
    }

    /// Creates a new task to explore all possible transformations for a logical group.
    ///
    /// This schedules jobs to apply all available transformation rules to all
    /// logical expressions in the group.
    async fn create_group_exploration_task(
        &mut self,
        group_id: GroupId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        // let task_id = self.register_new_task(ExploreGroup(group_id), Some(parent_task_id));
        // self.group_exploration_task_index.insert(group_id, task_id);

        // // Create a transformation task for all expression-rule combinations.
        // let transformations = self.rule_book.get_transformations().to_vec();
        // let expressions = self.memo.get_all_logical_exprs(group_id).await?;

        // for expression_id in expressions {
        //     for rule in &transformations {
        //         self.create_transform_expression_task(
        //             rule.clone(),
        //             expression_id,
        //             group_id,
        //             task_id,
        //         )
        //         .await?;
        //     }
        // }

        // Ok(task_id)
        todo!()
    }

    async fn create_fork_logical_task(
        &mut self,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        group_id: GroupId,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        todo!()
    }

    async fn create_continue_with_logical_task(
        &mut self,
        expr_id: LogicalExpressionId,
        fork_out: TaskId,
    ) -> Result<TaskId, Error> {
        todo!()
    }

    async fn create_fork_costed_task(
        &mut self,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        goal_id: GoalId,
        budget: Cost,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        todo!()
    }

    async fn create_continue_with_costed_task(
        &mut self,
        expr_id: PhysicalExpressionId,
        cost: Cost,
        fork_out: TaskId,
    ) -> Result<TaskId, Error> {
        todo!()
    }

    /// Creates a new task to optimize a goal.
    ///
    /// This method creates and manages the tasks needed to optimize a goal by
    /// ensuring group exploration, creating implementation tasks, and processing goal members.
    async fn create_goal_optimize_task(
        &mut self,
        goal_id: GoalId,
        parent_task_id: TaskId,
    ) -> Result<TaskId, Error> {
        // let task_id = self.register_new_task(OptimizeGoal(goal_id), Some(parent_task_id));
        // self.goal_optimization_task_index.insert(goal_id, task_id);

        // // Creates implementation tasks for all expression-rule combinations.
        // let Goal(group_id, _) = self.memo.materialize_goal(goal_id).await?;
        // self.ensure_group_exploration_task(group_id, task_id)
        //     .await?;

        // // Creates implementation tasks for all logical expressions in the group.
        // let logical_expressions = self.memo.get_all_logical_exprs(group_id).await?;
        // let implementations = self.rule_book.get_implementations().to_vec();

        // for expr_id in logical_expressions {
        //     for rule in &implementations {
        //         self.create_implement_expression_task(rule.clone(), expr_id, goal_id, task_id)
        //             .await?;
        //     }
        // }

        // // Process all goal members: physical expressions and subgoals.
        // let goal_members = self.memo.get_all_goal_members(goal_id).await?;
        // for member in goal_members {
        //     match member {
        //         GoalMemberId::PhysicalExpressionId(expr_id) => {
        //             self.ensure_cost_expression_task(expr_id, task_id).await?;
        //         }
        //         GoalMemberId::GoalId(referenced_goal_id) => {
        //             self.ensure_goal_optimize_task(referenced_goal_id, task_id)
        //                 .await?;
        //         }
        //     }
        // }

        // Ok(task_id)
        todo!()
    }
}
