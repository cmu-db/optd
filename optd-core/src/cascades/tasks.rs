// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.


use super::{CascadesOptimizer, GroupId, Memo};
use crate::nodes::NodeType;

mod apply_rule;
mod explore_expr;
mod explore_group;
mod optimize_expr;
mod optimize_group;
mod optimize_inputs;

pub use optimize_group::OptimizeGroupTask;

pub trait Task<T: NodeType, M: Memo<T>>: 'static + Send + Sync {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>);
}

pub fn get_initial_task<T: NodeType, M: Memo<T>>(
    initial_task_id: usize,
    root_group_id: GroupId,
) -> Box<dyn Task<T, M>> {
    Box::new(OptimizeGroupTask::new(
        None,
        initial_task_id,
        root_group_id,
        None,
    ))
}
