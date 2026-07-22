//! Greedy Operator Ordering with exact DP improvement of bounded subtrees.

use super::candidate::{CandidateDraft, JoinSearch};
use super::dphyp::DPhyp;
use super::plan::PlanState;
use crate::OptimizeError;
use crate::analysis::connecting_edge_indices;
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, nodeset_singleton};

use super::OptimizeResult;
use super::graph::JoinGraph;

struct GooTree<C> {
    nodes: NodeSet,
    state: PlanState<C>,
    children: Option<(Box<Self>, Box<Self>)>,
}

struct GreedyChoice<C> {
    left_index: usize,
    right_index: usize,
    immediate_cost: C,
    candidate: CandidateDraft<C>,
}

/// Builds a cheap global bushy tree, then applies exact DPhyp to maximal subtrees no larger than
/// `exact_subproblem_size` (the GOO/DP, IDP-2-style strategy from Neumann and Radke).
pub(super) fn solve<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    exact_subproblem_size: usize,
) -> OptimizeResult<Option<PlanState<M::Cost>>> {
    let hypergraph = search.hypergraph();
    if hypergraph.nodes.is_empty() {
        return Ok(None);
    }

    let mut components = Vec::with_capacity(hypergraph.nodes.len());
    for node_id in 0..hypergraph.nodes.len() {
        components.push(GooTree {
            nodes: nodeset_singleton(node_id),
            state: search.leaf(node_id)?,
            children: None,
        });
    }

    let graph = JoinGraph::new(hypergraph);
    while components.len() > 1 {
        let mut best: Option<GreedyChoice<M::Cost>> = None;
        for left_index in 0..components.len() {
            for right_index in left_index + 1..components.len() {
                let left = &components[left_index];
                let right = &components[right_index];
                if !graph.connects(&left.nodes, &right.nodes) {
                    continue;
                }
                let edge_indices = connecting_edge_indices(&left.nodes, &right.nodes, hypergraph);
                let Some(candidate) = search.best_join_candidate(
                    &left.nodes,
                    &left.state,
                    &right.nodes,
                    &right.state,
                    edge_indices,
                )?
                else {
                    continue;
                };
                // GOO chooses the pair with the cheapest immediate join result/work, not the
                // cumulative cost already paid inside its components.
                let greedy_cost = search.immediate_cost(&candidate)?;
                if best
                    .as_ref()
                    .is_none_or(|current| search.is_better(&greedy_cost, &current.immediate_cost))
                {
                    best = Some(GreedyChoice {
                        left_index,
                        right_index,
                        immediate_cost: greedy_cost,
                        candidate,
                    });
                }
            }
        }

        let Some(best) = best else {
            return Ok(None);
        };
        let right = components.remove(best.right_index);
        let left = components.remove(best.left_index);
        let state = search.commit(best.candidate);
        components.push(GooTree {
            nodes: &left.nodes | &right.nodes,
            state,
            children: Some((Box::new(left), Box::new(right))),
        });
    }

    improve_subtrees(
        components.pop().expect("a non-empty join graph has a tree"),
        search,
        exact_subproblem_size,
    )
    .map(Some)
}

fn improve_subtrees<M: CostModel>(
    tree: GooTree<M::Cost>,
    search: &mut JoinSearch<'_, M>,
    exact_subproblem_size: usize,
) -> OptimizeResult<PlanState<M::Cost>> {
    if tree.nodes.len() <= exact_subproblem_size {
        let mut exact = DPhyp::new(search);
        if let Some(plan) = exact.solve_subset(&tree.nodes)? {
            return Ok(plan);
        }
    }

    let Some((left, right)) = tree.children else {
        return Ok(tree.state);
    };
    let left_nodes = left.nodes.clone();
    let right_nodes = right.nodes.clone();
    let left = improve_subtrees(*left, search, exact_subproblem_size)?;
    let right = improve_subtrees(*right, search, exact_subproblem_size)?;
    let edge_indices = connecting_edge_indices(&left_nodes, &right_nodes, search.hypergraph());
    let candidate = search
        .best_join_candidate(&left_nodes, &left, &right_nodes, &right, edge_indices)?
        .ok_or_else(|| OptimizeError::PassError {
            pass: "JoinOrdering",
            message: "GOO subtree lost its connecting edge during DP improvement".to_string(),
        })?;
    Ok(search.commit(candidate))
}
