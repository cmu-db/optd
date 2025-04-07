use crate::{
    cir::{Cost, Goal, GoalId, GoalMemberId, PhysicalExpressionId},
    error::Error,
    memo::Memoize,
    optimizer::{Optimizer, Task},
};

use super::{SourceTaskId, TaskId};

/// Task data for optimizing a specific goal.
#[derive(Debug)]
pub struct OptimizeGoalTask {
    /// The goal to optimize.
    pub goal_id: GoalId,
    pub optimize_plan_out: Vec<TaskId>,
    pub optimize_goal_out: Vec<TaskId>,
    pub fork_costed_out: Vec<TaskId>,
    pub optimize_goal_in: Vec<TaskId>,
    pub explore_group_in: TaskId,
    pub implement_expression_in: Vec<TaskId>,
    pub cost_expression_in: Vec<TaskId>,
}

impl OptimizeGoalTask {
    /// Creates a new `OptimizeGoalTask` and track `out` as a subscriber.
    pub fn new(goal_id: GoalId, out: SourceTaskId, explore_group_in: TaskId) -> Self {
        let mut task = Self {
            goal_id,
            optimize_plan_out: Vec::new(),
            optimize_goal_out: Vec::new(),
            fork_costed_out: Vec::new(),
            optimize_goal_in: Vec::new(),
            explore_group_in,
            implement_expression_in: Vec::new(),
            cost_expression_in: Vec::new(),
        };
        task.add_subscriber(out);
        task
    }

    fn add_subscriber(&mut self, out: SourceTaskId) {
        match out {
            SourceTaskId::OptimizePlan(task_id) => self.optimize_plan_out.push(task_id),
            SourceTaskId::OptimizeGoal(task_id) => self.optimize_goal_out.push(task_id),
            SourceTaskId::ForkCosted(task_id) => self.fork_costed_out.push(task_id),
            _ => panic!("Expected OptimizePlan, OptimizeGoal, or ContinueWithCosted task"),
        }
    }

    /// Adds an `ImplementExpression` task as a dependency.
    pub fn add_implement_expr_in(&mut self, task_id: TaskId) {
        self.implement_expression_in.push(task_id);
    }

    /// Adds a `CostExpression` task as a dependency.
    fn add_cost_expr_in(&mut self, task_id: TaskId) {
        self.cost_expression_in.push(task_id);
    }

    /// Adds an `OptimizeGoal` task as a dependency.
    fn add_optimize_goal_in(&mut self, task_id: TaskId) {
        self.optimize_goal_in.push(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    pub async fn ensure_optimize_goal_task(
        &mut self,
        goal_id: GoalId,
        out: SourceTaskId,
    ) -> Result<(TaskId, Option<(PhysicalExpressionId, Cost)>), Error> {
        // Need Box::pin to avoid an infinite sized future.
        let result = Box::pin(async {
            if let Some(task_id) = self.goal_optimization_task_index.get(&goal_id) {
                let task = self.tasks.get_mut(task_id).unwrap();
                let optimize_goal_task = task.as_optimize_goal_mut();
                optimize_goal_task.add_subscriber(out);
                let best_optimized = self.memo.get_best_optimized_physical_expr(goal_id).await?;
                Ok((*task_id, best_optimized))
            } else {
                self.create_optimize_goal_task(goal_id, out).await
            }
        })
        .await?;
        Ok(result)
    }
    pub async fn create_optimize_goal_task(
        &mut self,
        goal_id: GoalId,
        out: SourceTaskId,
    ) -> Result<(TaskId, Option<(PhysicalExpressionId, Cost)>), Error> {
        let task_id = self.next_task_id();

        let Goal(group_id, _) = self.memo.materialize_goal(goal_id).await?;
        let (explore_group_in, logical_expressions) = self
            .ensure_explore_group_task(group_id, SourceTaskId::OptimizeGoal(task_id))
            .await?;

        let mut task = OptimizeGoalTask::new(goal_id, out, explore_group_in);

        // Creates implementation tasks for all logical expressions in the group.
        let implementations = self.rule_book.get_implementations().to_vec();

        for expr_id in logical_expressions {
            for rule in &implementations {
                let impl_expr_task_id = self
                    .create_implement_expression_task(rule.clone(), expr_id, goal_id, task_id)
                    .await?;
                task.add_implement_expr_in(impl_expr_task_id);
            }
        }

        // Process all goal members: physical expressions and subgoals.
        let goal_members = self.memo.get_all_goal_members(goal_id).await?;
        for member in goal_members {
            match member {
                GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
                    let task_id = self
                        .ensure_cost_expression_task(physical_expr_id, Cost(f64::MAX), task_id)
                        .await?;
                    task.add_cost_expr_in(task_id);
                }
                GoalMemberId::GoalId(ref_goal_id) => {
                    let (optimize_goal_task_id, _) = self
                        .ensure_optimize_goal_task(ref_goal_id, SourceTaskId::OptimizeGoal(task_id))
                        .await?;

                    task.add_optimize_goal_in(optimize_goal_task_id);
                }
            };
        }

        // insert into goal task index
        self.goal_optimization_task_index.insert(goal_id, task_id);
        self.tasks.insert(task_id, Task::OptimizeGoal(task));

        let best_costed_for_goal = self.memo.get_best_optimized_physical_expr(goal_id).await?;

        Ok((task_id, best_costed_for_goal))
    }
}
