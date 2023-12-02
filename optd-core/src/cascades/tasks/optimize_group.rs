use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        optimizer::GroupId,
        tasks::{optimize_expression::OptimizeExpressionTask, OptimizeInputsTask},
        CascadesOptimizer,
    },
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct OptimizeGroupTask {
    group_id: GroupId,
}

impl OptimizeGroupTask {
    pub fn new(group_id: GroupId) -> Self {
        Self { group_id }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeGroupTask {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(event = "task_begin", task = "optimize_group", group_id = %self.group_id);
        let group_info = optimizer.get_group_info(self.group_id);
        if group_info.winner.is_some() {
            trace!(event = "task_finish", task = "optimize_group");
            return Ok(vec![]);
        }
        let exprs = optimizer.get_all_exprs_in_group(self.group_id);
        let mut tasks = vec![];
        let exprs_cnt = exprs.len();
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if typ.is_logical() {
                tasks.push(Box::new(OptimizeExpressionTask::new(expr, false)) as Box<dyn Task<T>>);
            }
        }
        for &expr in &exprs {
            let typ = optimizer.get_expr_memoed(expr).typ.clone();
            if !typ.is_logical() {
                tasks.push(Box::new(OptimizeInputsTask::new(expr, true)) as Box<dyn Task<T>>);
            }
        }
        trace!(event = "task_finish", task = "optimize_group", group_id = %self.group_id, exprs_cnt = exprs_cnt);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!("optimize_group {}", self.group_id)
    }
}
