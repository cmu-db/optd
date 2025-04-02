use crate::{
    cir::{LogicalExpressionId, TransformationRule},
    error::Error,
    memo::Memoize,
    optimizer::Optimizer,
};

use super::TaskId;

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
}

impl<M: Memoize> Optimizer<M> {
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
        out: TaskId,
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
        //         job_id,
        //     );
        // }
        // Ok(task_id)
        todo!()
    }
}
