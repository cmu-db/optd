use crate::{cir::GoalId, memo::Memoize, optimizer::Optimizer};

use super::TaskId;

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

impl<M: Memoize> Optimizer<M> {
    pub fn create_optimize_goal_task(&mut self, goal_id: GoalId, initial_out: TaskId) -> TaskId {
        // task = get_task(task_id)
        // match task

        // task_id = next_task_id()

        // insert into goal task index
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
