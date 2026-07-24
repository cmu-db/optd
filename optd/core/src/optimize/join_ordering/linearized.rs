//! IKKBZ search-space linearization followed by interval dynamic programming.
//!
//! The configured [`CostModel`] is deliberately used only by interval DP. Linearization uses the
//! ASI-compatible `C_out` surrogate from Neumann and Radke: pair cardinalities determine
//! selectivity weights, a minimum spanning tree removes cycles, and IKKBZ finds the optimal
//! left-deep order of that tree under `C_out`.

mod asi;
mod ikkbz;

use std::collections::HashMap;

use super::candidate::{CandidateDraft, JoinSearch};
#[cfg(test)]
use super::plan::PlanState;
use super::plan::{PlanAtom, SolveOutcome};
use crate::cost::CostModel;
use crate::hypergraph::NodeSet;
use crate::optimize::OptimizeError;

use super::OptimizeResult;
use super::graph::JoinGraph;
use ikkbz::{RegularEdge, Relation};

#[cfg(test)]
pub(super) fn solve<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
) -> OptimizeResult<Option<PlanState<M::Cost>>> {
    Ok(solve_with_stats(search)?.plan)
}

/// Solves the full join group and reports the number of interval-DP states produced.
pub(super) fn solve_with_stats<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
) -> OptimizeResult<SolveOutcome<M::Cost>> {
    let all = NodeSet::all(search.hypergraph().nodes.len());
    solve_subset_with_stats(search, &all)
}

/// Solves the subgraph induced by `allowed` without renumbering its hypergraph nodes.
///
/// This entry point is used by GOO/DP to improve bounded subtrees while charging the number of
/// unique interval-table entries against its global DP budget.
pub(super) fn solve_subset_with_stats<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    allowed: &NodeSet,
) -> OptimizeResult<SolveOutcome<M::Cost>> {
    let frontier = allowed
        .iter()
        .map(|node| {
            search.leaf(node).map(|state| PlanAtom {
                nodes: NodeSet::singleton(node),
                state,
            })
        })
        .collect::<OptimizeResult<Vec<_>>>()?;
    solve_frontier_with_stats(search, &frontier)
}

/// Solves a contracted frontier while keeping every input atom opaque.
///
/// Each atom covers one or more original hypergraph nodes and carries the already optimized state
/// for that coverage. Interval DP may combine atoms but never split one, which is the contraction
/// required by the paper's budgeted GOO/DP scheduler.
pub(super) fn solve_frontier_with_stats<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    frontier: &[PlanAtom<M::Cost>],
) -> OptimizeResult<SolveOutcome<M::Cost>> {
    if frontier.is_empty() {
        return Ok(SolveOutcome {
            plan: None,
            dp_states_created: 0,
        });
    }
    validate_frontier(frontier)?;

    let graph = JoinGraph::new(search.hypergraph());
    let order = paper_linearization(search, frontier, &graph)?;
    let interval_sets = interval_sets(&order, frontier);
    let mut table = HashMap::with_capacity(order.len().saturating_mul(order.len()));

    for (position, atom) in order.iter().copied().enumerate() {
        table.insert((position, position), frontier[atom].state.clone());
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
                let edge_indices = graph.connecting_edge_indices(left_nodes, right_nodes);
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

    Ok(SolveOutcome {
        plan: table.get(&(0, order.len() - 1)).cloned(),
        dp_states_created: table.len(),
    })
}

/// Derives the paper's selectivity-weighted MST and IKKBZ order.
fn paper_linearization<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    frontier: &[PlanAtom<M::Cost>],
    graph: &JoinGraph<'_>,
) -> OptimizeResult<Vec<usize>> {
    // A frontier is conceptually unordered. Canonicalize it by the smallest covered original
    // relation so rank ties cannot make the result depend on the caller's traversal order.
    let mut canonical_atoms = (0..frontier.len()).collect::<Vec<_>>();
    canonical_atoms.sort_unstable_by_key(|atom| {
        frontier[*atom]
            .nodes
            .min()
            .expect("validated frontier atoms are non-empty")
    });
    let relations = canonical_atoms
        .iter()
        .enumerate()
        .map(|(position, atom)| {
            let cardinality = frontier[*atom]
                .state
                .properties
                .cardinality
                .as_deref()
                .ok_or_else(missing_cardinality)?
                .rows
                .value;
            Ok(Relation {
                node: position,
                cardinality,
            })
        })
        .collect::<OptimizeResult<Vec<_>>>()?;

    let mut edges = Vec::new();
    for left in 0..canonical_atoms.len() {
        for right in left + 1..canonical_atoms.len() {
            let left_atom = canonical_atoms[left];
            let right_atom = canonical_atoms[right];
            let left_input = &frontier[left_atom];
            let right_input = &frontier[right_atom];
            let edge_indices = graph.connecting_edge_indices(&left_input.nodes, &right_input.nodes);
            if edge_indices.is_empty() {
                continue;
            }
            let candidate = search
                .best_join_candidate(
                    &left_input.nodes,
                    &left_input.state,
                    &right_input.nodes,
                    &right_input.state,
                    edge_indices,
                )?
                .ok_or_else(|| OptimizeError::PassError {
                    pass: "JoinOrdering",
                    message: format!(
                        "connected frontier atoms {left_atom} and {right_atom} produced no join candidate"
                    ),
                })?;
            let selectivity = estimated_selectivity(
                candidate.output_rows()?,
                relations[left].cardinality,
                relations[right].cardinality,
            );
            edges.push(RegularEdge {
                left,
                right,
                selectivity,
            });
        }
    }

    // A repair frontier is normally connected. Retaining the deterministic walk as a defensive
    // fallback also makes callers robust to an incomplete contracted edge set.
    Ok(ikkbz::linearize(&relations, &edges).map_or_else(
        || connected_linearization(graph, frontier, &canonical_atoms),
        |order| {
            order
                .into_iter()
                .map(|position| canonical_atoms[position])
                .collect()
        },
    ))
}

fn validate_frontier<C>(frontier: &[PlanAtom<C>]) -> OptimizeResult<()> {
    let mut covered = NodeSet::EMPTY;
    for (atom, input) in frontier.iter().enumerate() {
        if input.nodes.is_empty() {
            return Err(OptimizeError::PassError {
                pass: "JoinOrdering",
                message: format!("linearized frontier atom {atom} covers no relations"),
            });
        }
        if !covered.is_disjoint(&input.nodes) {
            return Err(OptimizeError::PassError {
                pass: "JoinOrdering",
                message: format!("linearized frontier atom {atom} overlaps an earlier atom"),
            });
        }
        covered |= &input.nodes;
    }
    Ok(())
}

fn missing_cardinality() -> OptimizeError {
    OptimizeError::PassError {
        pass: "JoinOrdering",
        message: "IKKBZ requires cardinality profiles for every relation".to_string(),
    }
}

fn estimated_selectivity(output_rows: f64, left_rows: f64, right_rows: f64) -> f64 {
    let cross_product_rows = left_rows * right_rows;
    let selectivity = output_rows / cross_product_rows;
    if selectivity.is_finite() && !selectivity.is_sign_negative() {
        selectivity
    } else if output_rows == 0.0 && cross_product_rows > 0.0 {
        0.0
    } else {
        // Zero and overflowing inputs do not identify a stable ratio. A neutral edge weight keeps
        // the MST deterministic; C_out still observes the leaf cardinalities themselves.
        1.0
    }
}

/// Builds a deterministic order in which every prefix is connected. Interval DP therefore always
/// retains at least the left-deep plan induced by the order, while considering bushy subchains.
fn connected_linearization<C>(
    graph: &JoinGraph<'_>,
    frontier: &[PlanAtom<C>],
    canonical_atoms: &[usize],
) -> Vec<usize> {
    let mut order = Vec::with_capacity(frontier.len());
    let mut visited = NodeSet::EMPTY;

    while order.len() < frontier.len() {
        let atom = canonical_atoms
            .iter()
            .copied()
            .filter(|atom| !order.contains(atom))
            .find(|atom| visited.is_empty() || graph.connects(&visited, &frontier[*atom].nodes))
            .or_else(|| {
                canonical_atoms
                    .iter()
                    .copied()
                    .find(|atom| !order.contains(atom))
            })
            .expect("an incomplete linearization has an unvisited atom");
        visited |= &frontier[atom].nodes;
        order.push(atom);
    }
    order
}

fn interval_sets<C>(order: &[usize], frontier: &[PlanAtom<C>]) -> Vec<Vec<NodeSet>> {
    (0..order.len())
        .map(|start| {
            let mut set = NodeSet::EMPTY;
            (0..order.len())
                .map(|end| {
                    if end >= start {
                        set |= &frontier[order[end]].nodes;
                    }
                    set.clone()
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::DefaultCostModel;
    use crate::hypergraph::{Hyperedge, HyperedgeJoinType, HypergraphNode, QueryHypergraph};
    use crate::{CrossProduct, OperatorData, QueryContext, Scan, TableRef, nodeset_singleton};

    fn chain_graph() -> (QueryContext, QueryHypergraph) {
        let mut ctx = QueryContext::new();
        let roots = (0..3)
            .map(|node| {
                OperatorData::Scan(Scan {
                    table: TableRef::bare(format!("t{node}")),
                    columns: Vec::new(),
                })
                .add(&mut ctx)
            })
            .collect::<Vec<_>>();
        let nodes = roots
            .iter()
            .enumerate()
            .map(|(node, root)| HypergraphNode {
                root: *root,
                label: format!("t{node}"),
                available: Vec::new(),
            })
            .collect();
        let edges = [(0, 1), (1, 2)]
            .into_iter()
            .map(|(left, right)| Hyperedge {
                predicate: None,
                left: nodeset_singleton(left),
                right: nodeset_singleton(right),
                source: OperatorData::CrossProduct(CrossProduct {
                    outer: roots[left],
                    inner: roots[right],
                })
                .add(&mut ctx),
                join_type: HyperedgeJoinType::Inner,
            })
            .collect();
        (ctx, QueryHypergraph { nodes, edges })
    }

    #[test]
    fn contracted_frontier_remains_atomic_and_reports_table_entries() {
        let (mut ctx, hypergraph) = chain_graph();
        let mut analyses = crate::test_analyses(&ctx);
        let model = DefaultCostModel;
        let evaluator = super::super::evaluator::CardinalityEvaluator;
        let mut search = JoinSearch::new(&mut ctx, &mut analyses, &hypergraph, &model, &evaluator);

        let zero = search.leaf(0).unwrap();
        let one = search.leaf(1).unwrap();
        let zero_nodes = nodeset_singleton(0);
        let one_nodes = nodeset_singleton(1);
        let zero_one = search
            .best_join_candidate(&zero_nodes, &zero, &one_nodes, &one, vec![0])
            .unwrap()
            .map(|candidate| search.commit(candidate))
            .expect("the first edge connects nodes zero and one");
        let two = search.leaf(2).unwrap();
        let zero_one_nodes = &zero_nodes | &one_nodes;
        let frontier = [
            PlanAtom {
                nodes: nodeset_singleton(2),
                state: two,
            },
            PlanAtom {
                nodes: zero_one_nodes.clone(),
                state: zero_one,
            },
        ];

        let outcome = solve_frontier_with_stats(&mut search, &frontier).unwrap();
        let plan = outcome.plan.expect("the contracted chain is connected");
        let reversed = [frontier[1].clone(), frontier[0].clone()];
        let reversed_plan = solve_frontier_with_stats(&mut search, &reversed)
            .unwrap()
            .plan
            .expect("frontier order does not affect connectivity");

        assert_eq!(outcome.dp_states_created, 3);
        assert_eq!(plan.tree.leaf_count(), 3);
        assert!(plan.tree.has_join_with_leaves(&zero_one_nodes));
        assert_eq!(plan.tree, reversed_plan.tree);
    }

    #[test]
    fn invalid_selectivity_estimates_use_a_neutral_edge_weight() {
        assert_eq!(estimated_selectivity(f64::NAN, 10.0, 10.0), 1.0);
        assert_eq!(estimated_selectivity(f64::INFINITY, 10.0, 10.0), 1.0);
        assert_eq!(estimated_selectivity(10.0, f64::NAN, 10.0), 1.0);
        assert_eq!(estimated_selectivity(10.0, -1.0, 10.0), 1.0);

        assert_eq!(estimated_selectivity(0.0, f64::INFINITY, 10.0), 0.0);
        assert_eq!(estimated_selectivity(0.0, 10.0, 10.0), 0.0);
    }
}
