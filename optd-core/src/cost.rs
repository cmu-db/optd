use crate::{
    cascades::RelNodeContext,
    rel_node::{RelNode, RelNodeTyp, Value},
};

#[derive(Default, Clone, Debug, PartialOrd, PartialEq)]
pub struct Cost(pub Vec<f64>);

pub trait CostModel<T: RelNodeTyp>: 'static + Send + Sync {
    fn compute_cost(
        &self,
        node: &T,
        data: &Option<Value>,
        children: &[Cost],
        context: Option<RelNodeContext>,
    ) -> Cost;

    fn compute_plan_node_cost(&self, node: &RelNode<T>) -> Cost;

    fn explain(&self, cost: &Cost) -> String;

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost);

    fn zero(&self) -> Cost;
}
