use tracing::trace;

use crate::{
    cascades::{CascadesOptimizer, GroupId, Memo},
    nodes::NodeType,
};

use super::{explore_expr::ExploreExprTask, Task};

pub struct ExploreGroupTask {
    parent_task_id: Option<usize>,
    task_id: usize,
    group_id: GroupId,
    cost_limit: Option<isize>,
}

impl ExploreGroupTask {
    pub fn new(
        parent_task_id: Option<usize>,
        task_id: usize,
        group_id: GroupId,
        cost_limit: Option<isize>,
    ) -> Self {
        Self {
            parent_task_id,
            task_id,
            group_id,
            cost_limit,
        }
    }
}

/// ExploreGroup will apply transformation rules to generate more logical
/// expressions (or "explore" more logical expressions). It does this by
/// invoking the ExploreExpr task on every expression in the group.
/// (Recall "transformation rules" are logical -> logical)
///
/// Pseudocode:
/// function ExplGrp(grp, limit)
///     grp.Explored ← true
///     for expr ∈ grp.Expressions do
///         tasks.Push(ExplExpr(expr, limit))
impl<T: NodeType, M: Memo<T>> Task<T, M> for ExploreGroupTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) {
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_begin", task = "explore_group", group_id = %self.group_id);
        optimizer.mark_group_explored(self.group_id);
        for expr in optimizer.get_all_exprs_in_group(self.group_id) {
            optimizer.push_task(Box::new(ExploreExprTask::new(
                Some(self.task_id),
                optimizer.get_next_task_id(),
                expr,
                self.cost_limit,
            )));
        }
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_finish", task = "explore_group", group_id = %self.group_id);
    }
}
