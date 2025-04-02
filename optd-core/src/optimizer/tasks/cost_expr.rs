use crate::{
    cir::{Cost, PhysicalExpressionId},
    error::Error,
    memo::Memoize,
    optimizer::Optimizer,
};

use super::TaskId;

/// Task data for costing a physical expression.
pub(super) struct CostExpressionTask {
    /// The physical expression to cost.
    pub expr_id: PhysicalExpressionId,

    /// Whether the task has started the cost estimation.
    pub is_processing: bool,

    pub budget: Cost,

    pub optimize_goal_out: Vec<TaskId>,

    pub fork_in: Option<TaskId>,
}

impl<M: Memoize> Optimizer<M> {
    /// Ensures a cost expression task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to cost a physical expression as part of its work.
    /// If a costing task already exists, we reuse it.
    pub(super) async fn ensure_cost_expression_task(
        &mut self,
        expression_id: PhysicalExpressionId,
        budget: Cost,
        out: TaskId,
    ) -> Result<(), Error> {
        // TODO(yuchen): need to know what type of out this is.
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

        // Ok(())
        todo!()
    }
}
