//! Connectivity-preserving linearization followed by interval dynamic programming.

use std::collections::HashMap;

use super::candidate::{CandidateDraft, JoinSearch};
use super::plan::PlanState;
use crate::analysis::connecting_edge_indices;
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, QueryHypergraph, nodeset_singleton};

use super::OptimizeResult;
use super::graph::JoinGraph;

pub(super) fn solve<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
) -> OptimizeResult<Option<PlanState<M::Cost>>> {
    let hypergraph = search.hypergraph();
    if hypergraph.nodes.is_empty() {
        return Ok(None);
    }

    let order = connected_linearization(hypergraph);
    let interval_sets = interval_sets(&order);
    let mut table = HashMap::with_capacity(order.len() * order.len());

    for (position, node_id) in order.iter().copied().enumerate() {
        table.insert((position, position), search.leaf(node_id)?);
    }

    for width in 2..=order.len() {
        for start in 0..=order.len() - width {
            let end = start + width - 1;
            let mut best: Option<CandidateDraft<M::Cost>> = None;
            for split in start..end {
                let Some(left) = table.get(&(start, split)) else {
                    continue;
                };
                let Some(right) = table.get(&(split + 1, end)) else {
                    continue;
                };
                let left_nodes = &interval_sets[start][split];
                let right_nodes = &interval_sets[split + 1][end];
                let edge_indices = connecting_edge_indices(left_nodes, right_nodes, hypergraph);
                let Some(candidate) = search.best_join_candidate(
                    left_nodes,
                    left,
                    right_nodes,
                    right,
                    edge_indices,
                )?
                else {
                    continue;
                };
                if best
                    .as_ref()
                    .is_none_or(|current| search.is_better(candidate.cost(), current.cost()))
                {
                    best = Some(candidate);
                }
            }
            if let Some(best) = best {
                table.insert((start, end), search.commit(best));
            }
        }
    }

    Ok(table.remove(&(0, order.len() - 1)))
}

/// Builds a deterministic order in which every prefix is connected. Interval DP therefore always
/// retains at least the left-deep plan induced by the order, while considering bushy subchains.
fn connected_linearization(hypergraph: &QueryHypergraph) -> Vec<usize> {
    let graph = JoinGraph::new(hypergraph);
    let mut order = Vec::with_capacity(hypergraph.nodes.len());
    let mut visited = NodeSet::EMPTY;

    while order.len() < hypergraph.nodes.len() {
        let next = (0..hypergraph.nodes.len())
            .filter(|node| !visited.contains(*node))
            .find(|node| visited.is_empty() || graph.connects(&visited, &nodeset_singleton(*node)))
            .or_else(|| (0..hypergraph.nodes.len()).find(|node| !visited.contains(*node)))
            .expect("an incomplete linearization has an unvisited node");
        visited |= &nodeset_singleton(next);
        order.push(next);
    }
    order
}

fn interval_sets(order: &[usize]) -> Vec<Vec<NodeSet>> {
    (0..order.len())
        .map(|start| {
            let mut set = NodeSet::EMPTY;
            (0..order.len())
                .map(|end| {
                    if end >= start {
                        set |= &nodeset_singleton(order[end]);
                    }
                    set.clone()
                })
                .collect()
        })
        .collect()
}
