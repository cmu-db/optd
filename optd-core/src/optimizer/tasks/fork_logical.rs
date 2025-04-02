use optd_dsl::{
    analyzer::hir::{GroupId, Value},
    engine::{Continuation, EngineResponse},
};

use crate::{
    memo::Memoize,
    optimizer::{EngineMessageKind, Optimizer},
};

use super::TaskId;

pub(super) struct ForkLogicalTask {
    pub continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,

    /// ContinueWithLogical | TransformExpression | ImplementExpression
    /// (memo alexis: I don't think these matter too much for continuations...)
    pub out: TaskId,
    pub explore_group_in: TaskId,
    pub continue_tasks_in: Vec<TaskId>,
}

impl<M: Memoize> Optimizer<M> {
    /// Creates a task to fork the logical plan for further exploration.
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    ///
    /// Only schedules the starting job if the transformation is marked as dirty in the memo.
    pub(super) async fn create_fork_logical_task_v2(
        &mut self,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        out: TaskId,
        group_id: GroupId,
    ) -> Result<TaskId, crate::error::Error> {
        todo!()

        // first: ensure_explore_group_task_v2(group_id, out).await?;
        // for all logical expressions in the group:
        //     create_continue_with_logical_task_v2(
        //         expression_id,
        //         task_id
        //     ).await?;
    }
}
