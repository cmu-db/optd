use optd_dsl::{
    analyzer::hir::Value,
    engine::{Continuation, EngineResponse},
};

use crate::{cir::Cost, optimizer::EngineMessageKind};

use super::TaskId;

pub(super) struct ForkCostedTask {
    pub continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,

    /// The current upper bound on the allowed cost budget.
    pub budget: Cost,

    /// ContinueWithCosted | CostExpresion
    /// (memo alexis: I don't think these matter too much for continuations...)
    pub out: TaskId,

    pub optimize_goal_in: TaskId,

    pub fork_in: Vec<TaskId>,
}

impl ForkCostedTask {
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
            fork_in: Vec::new(),
        }
    }
}
