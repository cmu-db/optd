use crate::plan_nodes::OptRelNodeTyp;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, Value},
};

/// Dummy cost model that returns a 0 cost in all cases.
/// Intended for testing with the cascades optimizer.
pub struct DummyCostModel;

impl CostModel<OptRelNodeTyp> for DummyCostModel {
    fn compute_cost(
        &self,
        _node: &OptRelNodeTyp,
        _data: &Option<Value>,
        _children: &[Cost],
        _context: Option<RelNodeContext>,
        _optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        Cost(vec![0.0])
    }

    fn compute_plan_node_cost(&self, _node: &RelNode<OptRelNodeTyp>) -> Cost {
        Cost(vec![0.0])
    }

    fn explain(&self, _node: &Cost) -> String {
        "Dummy cost".to_string()
    }

    fn accumulate(&self, _total_cost: &mut Cost, _cost: &Cost) {}

    fn zero(&self) -> Cost {
        Cost(vec![0.0])
    }
}
