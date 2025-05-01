use optd_dsl::{
    analyzer::hir::Value,
    engine::{Continuation, EngineResponse},
};

use crate::{
    cir::GroupId,
    memo::Memoize,
    optimizer::{EngineMessageKind, Optimizer},
};

use super::{SourceTaskId, Task, TaskId};

pub struct ForkLogicalTask {
    pub continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,

    /// ContinueWithLogical | TransformExpression | ImplementExpression
    /// (memo alexis: I don't think these matter too much for continuations...)
    pub out: TaskId,
    pub explore_group_in: TaskId,
    pub continue_ins: Vec<TaskId>,
}

impl ForkLogicalTask {
    /// Creates a new `ForkLogicalTask` and track `out` as a subscriber.
    pub fn new(
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        out: TaskId,
        explore_group_in: TaskId,
    ) -> Self {
        Self {
            continuation,
            out,
            explore_group_in,
            continue_ins: Vec::new(),
        }
    }

    pub fn add_continue_in(&mut self, task_id: TaskId) {
        self.continue_ins.push(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    /// Creates a task to fork the logical plan for further exploration.
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    ///
    /// Only schedules the starting job if the transformation is marked as dirty in the memo.
    pub async fn create_fork_logical_task(
        &mut self,
        group_id: GroupId,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        out: TaskId,
    ) -> Result<TaskId, crate::error::Error> {
        let task_id = self.next_task_id();
        let (explore_group_in, logical_expr_ids) = self
            .ensure_explore_group_task(group_id, SourceTaskId::ForkLogical(task_id))
            .await?;

        let mut task = ForkLogicalTask::new(continuation, out, explore_group_in);

        for logical_expr_id in logical_expr_ids {
            let cont_with_logical_task_id = self
                .create_continue_with_logical_task(logical_expr_id, task_id)
                .await?;
            task.add_continue_in(cont_with_logical_task_id);
        }

        self.tasks.insert(task_id, Task::ForkLogical(task));

        Ok(task_id)
    }
}

impl std::fmt::Debug for ForkLogicalTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkLogicalTask")
            .field("out", &self.out)
            .field("explore_group_in", &self.explore_group_in)
            .field("continue_ins", &self.continue_ins)
            .finish()
    }
}
