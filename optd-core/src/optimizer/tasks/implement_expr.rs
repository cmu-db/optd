use crate::{
    cir::{GoalId, ImplementationRule, LogicalExpressionId},
    error::Error,
    memo::Memoize,
    optimizer::Optimizer,
};

use super::TaskId;

/// Task data for implementing a logical expression using a specific rule.
pub(super) struct ImplementExpressionTask {
    /// The implementation rule to apply.
    pub rule: ImplementationRule,

    /// Whether the task has started the implementation rule.
    pub is_processing: bool,

    /// The logical expression to implement.
    pub expression_id: LogicalExpressionId,

    pub optimize_goal_out: TaskId,

    pub fork_in: Option<TaskId>,
}

impl<M: Memoize> Optimizer<M> {
    async fn create_implement_expression_task_v2(
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
        //        job_id
        //     );
        // }

        // Ok(task_id)
        todo!()
    }
}
