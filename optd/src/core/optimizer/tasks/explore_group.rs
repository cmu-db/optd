use crate::{
    core::cir::{GroupId, LogicalExpressionId},
    core::error::Error,
    core::memo::Memoize,
    core::optimizer::Optimizer,
};

use super::{SourceTaskId, Task, TaskId};

/// Task data for exploring a group.
#[derive(Debug)]
pub struct ExploreGroupTask {
    /// The group to explore.
    pub group_id: GroupId,
    pub optimize_goal_out: Vec<TaskId>,
    pub fork_logical_out: Vec<TaskId>,
    pub transform_expr_in: Vec<TaskId>,
}

impl ExploreGroupTask {
    /// Creates a new `ExploreGroupTask`.
    pub fn new(group_id: GroupId, out_task: Option<SourceTaskId>) -> Self {
        let mut task = Self {
            group_id,
            optimize_goal_out: Vec::new(),
            fork_logical_out: Vec::new(),
            transform_expr_in: Vec::new(),
        };
        if let Some(out_task) = out_task {
            task.add_subscriber(out_task);
        }
        task
    }

    fn add_subscriber(&mut self, out: SourceTaskId) {
        match out {
            SourceTaskId::ForkLogical(task_id) => self.fork_logical_out.push(task_id),
            SourceTaskId::OptimizeGoal(task_id) => self.optimize_goal_out.push(task_id),
            _ => panic!("Expected ForkLogical or OptimizeGoal task"),
        }
    }

    pub fn add_transform_expr_in(&mut self, task_id: TaskId) {
        self.transform_expr_in.push(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    // Creates the `ExploreGroup` task if it does not exist and get all current logical expressions.
    pub async fn ensure_explore_group_task(
        &mut self,
        group_id: GroupId,
        out: SourceTaskId,
    ) -> Result<(TaskId, Vec<LogicalExpressionId>), Error> {
        if let Some(task_id) = self.group_exploration_task_index.get(&group_id) {
            let task = self.tasks.get_mut(task_id).unwrap();

            let explore_group_task = task.as_explore_group_mut();
            explore_group_task.add_subscriber(out);
            let logical_expr_ids = self.memo.get_all_logical_exprs(group_id).await?;
            Ok((*task_id, logical_expr_ids))
        } else {
            self.create_explore_group_task(group_id, Some(out)).await
        }
    }

    /// Creates a task to explore a group.
    pub async fn create_explore_group_task(
        &mut self,
        group_id: GroupId,
        out: Option<SourceTaskId>,
    ) -> Result<(TaskId, Vec<LogicalExpressionId>), Error> {
        let task_id = self.next_task_id();
        let mut task = ExploreGroupTask::new(group_id, out);

        // Create a transformation task for all expression-rule combinations.
        let rule_book = &self.rule_book;
        let memo = &self.memo;
        let transformations = rule_book.get_transformations().to_vec();
        let logical_expr_ids = memo.get_all_logical_exprs(group_id).await?;

        for logical_expr_id in logical_expr_ids.iter() {
            for rule in &transformations {
                let task_id = self
                    .create_transform_expression_task(rule.clone(), *logical_expr_id, task_id)
                    .await?;
                task.add_transform_expr_in(task_id);
            }
        }
        // Register the task and add it to the group_exploration index.
        let tasks = &mut self.tasks;
        let group_exploration_task_index = &mut self.group_exploration_task_index;
        tasks.insert(task_id, Task::ExploreGroup(task));
        group_exploration_task_index.insert(group_id, task_id);
        Ok((task_id, logical_expr_ids))
    }
}
