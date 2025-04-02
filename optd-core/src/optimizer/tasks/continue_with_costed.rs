use crate::cir::PhysicalExpressionId;

use super::TaskId;

pub(super) struct ContinueWithCostedTask {
    pub expr_id: PhysicalExpressionId,
    pub fork_out: TaskId,
    pub fork_in: Option<TaskId>,
}

impl ContinueWithCostedTask {
    pub fn new(expr_id: PhysicalExpressionId, fork_out: TaskId) -> Self {
        Self {
            expr_id,
            fork_out,
            fork_in: None,
        }
    }
}
