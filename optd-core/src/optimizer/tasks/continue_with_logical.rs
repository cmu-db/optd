use crate::cir::LogicalExpressionId;

use super::TaskId;

pub(super) struct ContinueWithLogicalTask {
    pub expr_id: LogicalExpressionId,
    pub fork_out: TaskId,
    pub fork_in: Option<TaskId>,
}

impl ContinueWithLogicalTask {
    pub fn new(expr_id: LogicalExpressionId, fork_out: TaskId) -> Self {
        Self {
            expr_id,
            fork_out,
            fork_in: None,
        }
    }
}
