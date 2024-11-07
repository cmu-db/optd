use anyhow::Result;

use crate::nodes::{ArcPlanNode, NodeType, PlanNodeOrGroup};
use crate::property::PropertyBuilder;

pub trait Optimizer<T: NodeType> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>>;
    fn get_property<P: PropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop;
}
