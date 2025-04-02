use crate::{
    cir::{GroupId, LogicalExpressionId},
    memo::Memoize,
    optimizer::Optimizer,
};

use super::{Task, TaskId};

pub enum ExploreGroupSubscriberTaskType {
    ForkLogical,
    OptimizeGoal,
}

/// Task data for exploring a group.
pub(super) struct ExploreGroupTask {
    /// The group to explore.
    pub group_id: GroupId,
    pub optimize_goal_out: Vec<TaskId>,
    pub fork_logical_out: Vec<TaskId>,
    pub transform_expr_in: Vec<TaskId>,
}

impl<M: Memoize> Optimizer<M> {
    pub async fn create_explore_group_task(
        &mut self,
        group_id: GroupId,
        initial_out: TaskId,
        subscriber_task_type: &ExploreGroupSubscriberTaskType,
    ) -> (TaskId, &mut ExploreGroupTask) {
        let task_id = self.next_task_id;
        self.next_task_id = TaskId(self.next_task_id.0 + 1);

        self.group_exploration_task_index.insert(group_id, task_id);
        // Create a transformation task for all expression-rule combinations.
        let transformations = self.rule_book.get_transformations().to_vec();
        let expressions = self.memo.get_all_logical_exprs(group_id).await.expect(
            "Failed to get all logical expressions for the explore group task during creation",
        );

        let mut optimize_goal_out = Vec::new();
        let mut fork_logical_out = Vec::new();
        let mut transform_expr_in = Vec::new();

        match subscriber_task_type {
            ExploreGroupSubscriberTaskType::ForkLogical => {
                fork_logical_out.push(initial_out);
            }
            ExploreGroupSubscriberTaskType::OptimizeGoal => {
                optimize_goal_out.push(initial_out);
            }
        }

        for expression_id in expressions {
            for rule in &transformations {
                todo!()
                // transform_expr_in.push(self.create_transform_expression_task(
                //     rule.clone(),
                //     expression_id,
                //     group_id,
                //     task_id,
                // )
                // .await?;
            }
        }

        let explore_group_task = ExploreGroupTask {
            group_id,
            optimize_goal_out,
            fork_logical_out,
            transform_expr_in,
        };

        let explore_group_task = self
            .task_index
            .entry(task_id)
            .or_insert(Task::ExploreGroup(explore_group_task));

        let explore_group_task = match explore_group_task {
            Task::ExploreGroup(task) => task,
            _ => panic!("Expected ExploreGroup task"),
        };

        (task_id, explore_group_task)
    }

    pub fn group_receive_logical_expression(
        &self,
        group_id: GroupId,
        stored_logical_expr_id: LogicalExpressionId,
    ) {
        // launching of trasnform expression for new logical expresionn
        // call receive logical expression on all outs
    }

    // create the explore group if it does not exist, and then subscribe to it and get all current logical expressions
    pub async fn ensure_explore_group_task(
        &mut self,
        group_id: GroupId,
        out: TaskId,
        subscriber_task_type: ExploreGroupSubscriberTaskType,
    ) -> (TaskId, Vec<LogicalExpressionId>) {
        let (explore_group_task_id, explore_group_task) =
            if let Some(task_id) = self.group_exploration_task_index.get(&group_id) {
                let explore_group_task = self
                    .task_index
                    .get_mut(&task_id)
                    .expect("We just created the task, so it must exist");
                let explore_group_task = match explore_group_task {
                    Task::ExploreGroup(task) => task,
                    _ => panic!("Expected ExploreGroup task"),
                };
                (task_id.clone(), explore_group_task)
            } else {
                let (task_id, explore_group_task) = self
                    .create_explore_group_task(group_id, out, &subscriber_task_type)
                    .await;
                (task_id, explore_group_task)
            };

        match &subscriber_task_type {
            ExploreGroupSubscriberTaskType::ForkLogical => {
                explore_group_task.fork_logical_out.push(out);
            }
            ExploreGroupSubscriberTaskType::OptimizeGoal => {
                explore_group_task.optimize_goal_out.push(out);
            }
        }

        let logical_expressions = self.memo.get_all_logical_exprs(group_id).await.expect(
            "Failed to get all logical expressions for the explore group task during ensure",
        );

        (explore_group_task_id, logical_expressions)
    }
}
