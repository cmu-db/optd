use crate::dsl::{
    analyzer::hir::Value,
    engine::{Continuation, EngineResponse},
};

use crate::{
    core::cir::{Cost, GoalId},
    core::error::Error,
    memo::Memoize,
    core::optimizer::{EngineMessageKind, Optimizer},
};

use super::{SourceTaskId, Task, TaskId};

pub struct ForkCostedTask {
    pub continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,

    /// The current upper bound on the allowed cost budget.
    pub budget: Cost,

    /// ContinueWithCosted | CostExpresion
    /// (memo alexis: I don't think these matter too much for continuations...)
    pub out: TaskId,

    pub optimize_goal_in: TaskId,

    pub continue_ins: Vec<TaskId>,
}

impl ForkCostedTask {
    /// Creates a new `ForkCostedTask` and track `out` as a subscriber.
    pub fn new(
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        budget: Cost,
        out: TaskId,
        optimize_goal_in: TaskId,
    ) -> Self {
        Self {
            continuation,
            budget,
            out,
            optimize_goal_in,
            continue_ins: Vec::new(),
        }
    }

    pub fn add_continue_in(&mut self, task_id: TaskId) {
        self.continue_ins.push(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    pub(crate) async fn create_fork_costed_task(
        &mut self,
        goal_id: GoalId,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();
        let (optimize_goal_in, best_costed) = self
            .ensure_optimize_goal_task(goal_id, SourceTaskId::ForkCosted(task_id))
            .await?;

        let mut task = ForkCostedTask::new(continuation, Cost(f64::MAX), out, optimize_goal_in);

        if let Some((physical_expr_id, cost)) = best_costed {
            let cont_with_costed_task_id = self
                .create_continue_with_costed_task(physical_expr_id, cost, task_id)
                .await?;
            task.add_continue_in(cont_with_costed_task_id);
        }

        self.tasks.insert(task_id, Task::ForkCosted(task));

        Ok(task_id)
    }
}

impl std::fmt::Debug for ForkCostedTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkCostedTask")
            .field("out", &self.out)
            .field("optimize_goal_in", &self.optimize_goal_in)
            .field("continue_ins", &self.continue_ins)
            .finish()
    }
}
