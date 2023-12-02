use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        optimizer::{CascadesOptimizer, GroupId},
        tasks::OptimizeExpressionTask,
    },
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct ExploreGroupTask {
    group_id: GroupId,
}

impl ExploreGroupTask {
    pub fn new(group_id: GroupId) -> Self {
        Self { group_id }
    }
}

impl<T: RelNodeTyp> Task<T> for ExploreGroupTask {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(event = "task_begin", task = "explore_group", group_id = %self.group_id);
        let mut tasks = vec![];
        if optimizer.is_group_explored(self.group_id) {
            trace!(target: "task_finish", task = "explore_group", result = "already explored, skipping", group_id = %self.group_id);
            return Ok(vec![]);
        }
        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let exprs_cnt = exprs.len();
        for expr in exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(Box::new(OptimizeExpressionTask::new(expr, true)) as Box<dyn Task<T>>);
            }
        }
        optimizer.mark_group_explored(self.group_id);
        trace!(
            event = "task_finish",
            task = "explore_group",
            result = "expand group",
            exprs_cnt = exprs_cnt
        );
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("explore_group {}", self.group_id)
    }
}
