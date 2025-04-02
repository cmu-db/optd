use crate::{
    cir::{Goal, GoalId, GoalMemberId},
    memo::Memoize,
    optimizer::{Optimizer, Task},
};

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
    pub async fn create_optimize_goal_task(
        &mut self,
        goal_id: GoalId,
        initial_out: TaskId,
    ) -> TaskId {
        let parent_task = self
            .task_index
            .get(&initial_out)
            .expect("Parent task not found when creating optimize goal task");
        let task_id = self.next_task_id;
        self.next_task_id = TaskId(self.next_task_id.0 + 1);


        let Goal(group_id, _) = self.memo.materialize_goal(goal_id).await.expect("Failed to materialize goal for the optimize goal task");
        let (explore_group_task, logical_expressions) = self.ensure_explore_group_task(group_id, task_id).await;    

        // Creates implementation tasks for all logical expressions in the group.
        let implementations = self.rule_book.get_implementations().to_vec();
        let implement_expression_tasks = Vec::new();
        let cost_expression_tasks = Vec::new();
        let optimize_goal_tasks = Vec::new();


        for expr_id in logical_expressions {
            for rule in &implementations {
                implement_expression_tasks.push(self.create_implement_expression_task(rule.clone(), expr_id, goal_id, task_id)
                    .await.expect("Failed to create implement expression task for the optimize goal task"));
            }
        }

        // Process all goal members: physical expressions and subgoals.
        let goal_members = self.memo.get_all_goal_members(goal_id).await.expect("Failed to get all goal members for the optimize goal task");
        for member in goal_members {
            match member {
                GoalMemberId::PhysicalExpressionId(expr_id) => {
                    cost_expression_tasks.push(self.ensure_cost_expression_task(expr_id, task_id).await.expect("Failed to ensure cost expression task for the optimize goal task"));
                }
                GoalMemberId::GoalId(referenced_goal_id) => {
                    optimize_goal_task.push(self.ensure_goal_optimize_task(referenced_goal_id, task_id)
                        .await.expect("Failed to ensure goal optimize task for the optimize goal task"));
                }
            }
        }

        let optimize_goal_task = 
        match parent_task {
            Task::OptimizePlan(optimize_plan_task) => OptimizeGoalTask {
                goal_id,
                optimize_plan_out: vec![initial_out],
                optimize_goal_out: vec![],
                continue_with_out: vec![],
                optimize_goal_in: optimize_goal_tasks,
                explore_group_in: explore_group_task,
                implement_expression_in: implement_expression_tasks,
                cost_expression_in: cost_expression_tasks,
            },
            Task::OptimizeGoal(optimize_goal_task) => OptimizeGoalTask {
                goal_id,
                optimize_plan_out: vec![],
                optimize_goal_out: vec![initial_out],
                continue_with_out: vec![],
                optimize_goal_in: vec![],
                explore_group_in: vec![],
                implement_expression_in: vec![],
                cost_expression_in: vec![],
            },
            Task::ContinueWithCosted(continue_with_costed_task) => OptimizeGoalTask {
                goal_id,
                optimize_plan_out: vec![],
                optimize_goal_out: vec![],
                continue_with_out: vec![initial_out],
                optimize_goal_in: vec![],
                explore_group_in: ,
                implement_expression_in: vec![],
                cost_expression_in: vec![],
            },
            Task::ExploreGroup(_) => panic!("Why is explore group creating optimize goal tasks?"),
            Task::ImplementExpression(_) => panic!("Why is implement expression creating optimize goal tasks?"),
            Task::TransformExpression(_) => panic!("Why is transform expression creating optimize goal tasks?"),
            Task::CostExpression(_) => panic!("Why is cost expression creating optimize goal tasks?"),
            Task::ForkLogical(_) => panic!("Why is fork logical creating optimize goal tasks?"),
            Task::ContinueWithLogical(_) => panic!("Why is continue with logical creating optimize goal tasks?"),
            Task::ForkCosted(_) => panic!("Why is fork costed creating optimize goal tasks?"),
        };

        // insert into goal task index
        // self.goal_optimization_task_index.insert(goal_id, task_id);
        self.task_index.insert(
            task_id,
            Task::OptimizeGoal(optimize_goal_task),
        );

        task_id
    }
}
