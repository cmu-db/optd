// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

mod ir;

pub use ir::RuleMatcher;

use crate::nodes::{ArcPlanNode, NodeType, PlanNodeOrGroup};
use crate::optimizer::Optimizer;

// TODO: docs, possible renames.
// TODO: Why do we have all of these match types? Seems like possible overkill.
pub trait Rule<T: NodeType, O: Optimizer<T>>: 'static + Send + Sync {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, optimizer: &O, binding: ArcPlanNode<T>) -> Vec<PlanNodeOrGroup<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
