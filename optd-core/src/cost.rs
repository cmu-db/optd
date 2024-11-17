// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use crate::cascades::{CascadesOptimizer, Memo, RelNodeContext};
use crate::nodes::{ArcPredNode, NodeType};

/// The statistics of a group.
pub struct Statistics(pub Box<dyn std::any::Any + Send + Sync + 'static>);

/// The cost of an operation. The cost is represented as a vector of double values.
/// For example, it can be represented as `[compute_cost, io_cost]`.
/// A lower value means a better cost.
#[derive(Default, Clone, Debug, PartialOrd, PartialEq)]
pub struct Cost(pub Vec<f64>);

pub trait CostModel<T: NodeType, M: Memo<T>>: 'static + Send + Sync {
    /// Compute the cost of a single operation. `RelNodeContext` might be
    /// optional in the future when we implement physical property enforcers.
    /// If we have not decided the winner for a child group yet, the statistics
    /// for that group will be `None`.
    #[allow(clippy::too_many_arguments)]
    fn compute_operation_cost(
        &self,
        node: &T,
        predicates: &[ArcPredNode<T>],
        children_stats: &[Option<&Statistics>],
        context: RelNodeContext,
        optimizer: &CascadesOptimizer<T, M>,
    ) -> Cost;

    /// Derive the statistics of a single operation. `RelNodeContext` might be
    /// optional in the future when we implement physical property enforcers.
    fn derive_statistics(
        &self,
        node: &T,
        predicates: &[ArcPredNode<T>],
        children_stats: &[&Statistics],
        context: RelNodeContext,
        optimizer: &CascadesOptimizer<T, M>,
    ) -> Statistics;

    fn explain_cost(&self, cost: &Cost) -> String;

    fn explain_statistics(&self, cost: &Statistics) -> String;

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost);

    fn sum(&self, operation_cost: &Cost, inputs_cost: &[Cost]) -> Cost {
        let mut total_cost = operation_cost.clone();
        for input in inputs_cost {
            self.accumulate(&mut total_cost, input);
        }
        total_cost
    }

    /// The zero cost.
    fn zero(&self) -> Cost;

    /// The weighted cost of a compound cost.
    fn weighted_cost(&self, cost: &Cost) -> f64;
}
