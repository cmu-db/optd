use optimize_group::OptimizeGroupTask;

use crate::rel_node::RelNodeTyp;

mod apply_rule;
mod explore_expr;
mod explore_group;
mod optimize_expr;
mod optimize_group;
mod optimize_inputs;

use super::{CascadesOptimizer, GroupId};

pub trait Task<T: RelNodeTyp>: 'static + Send + Sync {
    fn execute(&self, optimizer: &CascadesOptimizer<T>);
}

pub fn get_initial_task<T: RelNodeTyp>(
    initial_task_id: usize,
    root_group_id: GroupId,
) -> Box<dyn Task<T>> {
    Box::new(OptimizeGroupTask::new(
        None,
        initial_task_id,
        root_group_id,
        None,
    ))
}
