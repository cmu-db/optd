use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{optimizer::ExprId, tasks::OptimizeGroupTask, CascadesOptimizer},
    rel_node::RelNodeTyp,
};

use super::Task;

#[derive(Debug)]
struct ContinueTask {
    next_group_idx: usize,
}

pub struct OptimizeInputsTask {
    expr_id: ExprId,
    continue_from: Option<ContinueTask>,
}

impl OptimizeInputsTask {
    pub fn new(expr_id: ExprId) -> Self {
        Self {
            expr_id,
            continue_from: None,
        }
    }

    fn continue_from(&self, cont: ContinueTask) -> Self {
        Self {
            expr_id: self.expr_id,
            continue_from: Some(cont),
        }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeInputsTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(event = "task_begin", task = "optimize_inputs", expr_id = %self.expr_id, continue_from = ?self.continue_from);
        let children = &optimizer.get_expr_memoed(self.expr_id).children;

        let tasks;

        if let Some(ContinueTask { next_group_idx }) = &self.continue_from {
            let next_group_idx = *next_group_idx;
            if next_group_idx < children.len() {
                tasks = vec![
                    Box::new(self.continue_from(ContinueTask {
                        next_group_idx: next_group_idx + 1,
                    })) as Box<dyn Task<T>>,
                    Box::new(OptimizeGroupTask::new(children[next_group_idx])) as Box<dyn Task<T>>,
                ];
            } else {
                tasks = vec![];
            }
        } else if !children.is_empty() {
            tasks = vec![
                Box::new(self.continue_from(ContinueTask { next_group_idx: 1 }))
                    as Box<dyn Task<T>>,
                Box::new(OptimizeGroupTask::new(children[0])) as Box<dyn Task<T>>,
            ];
        } else {
            tasks = vec![];
        }

        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
        Ok(tasks)
    }
}
