// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use anyhow::Result;

use crate::logical_property::LogicalPropertyBuilder;
use crate::nodes::{ArcPlanNode, NodeType, PlanNodeOrGroup};
use crate::physical_property::PhysicalProperty;

pub trait Optimizer<T: NodeType> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>>;

    fn optimize_with_required_props(
        &mut self,
        root_rel: ArcPlanNode<T>,
        required_props: &[&dyn PhysicalProperty],
    ) -> Result<ArcPlanNode<T>>;

    fn get_logical_property<P: LogicalPropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop;
}
