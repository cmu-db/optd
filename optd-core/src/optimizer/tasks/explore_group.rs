use crate::{
    cir::{GroupId, LogicalExpressionId},
    memo::Memoize,
    optimizer::Optimizer,
};

use super::TaskId;

/// Task data for exploring a group.
pub(super) struct ExploreGroupTask {
    /// The group to explore.
    pub group_id: GroupId,
    pub optimize_goal_out: Vec<TaskId>,
    pub fork_logical_out: Vec<TaskId>,
    pub transform_expr_in: Vec<TaskId>,
}

impl<M: Memoize> Optimizer<M> {
    pub fn create_explore_group_task(&mut self, group_id: GroupId, initial_out: TaskId) -> TaskId {
        // let task_id = self.next_task_id();
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

    fn receive_logical_expression(&self, stored_logical_expr_id: LogicalExpressionId) {
        // launching of trasnform expression for new logical expresionn
        // call receive logical expression on all outs
    }
}
