//! IKKBZ linearization for ordinary query graphs.
//!
//! A cyclic query graph is reduced to a selectivity-minimum spanning tree. IKKBZ then directs
//! that tree away from each possible start relation, normalizes rank-conflicting child chains
//! into compound relations, and merges independent chains by ascending `C_out` rank. The cheapest
//! rooted order under `C_out` becomes the linearization consumed by interval DP.

use std::collections::HashMap;

use super::asi::COutSummary;
use crate::disjoint_set::DisjointSet;

/// Cardinality information for one original hypergraph node.
#[derive(Debug, Clone, Copy)]
pub(super) struct Relation {
    pub(super) node: usize,
    pub(super) cardinality: f64,
}

/// An undirected ordinary-graph edge with the combined selectivity of its predicates.
#[derive(Debug, Clone, Copy)]
pub(super) struct RegularEdge {
    pub(super) left: usize,
    pub(super) right: usize,
    pub(super) selectivity: f64,
}

/// Produces the cheapest IKKBZ order, or `None` when the supplied graph is disconnected.
pub(super) fn linearize(relations: &[Relation], edges: &[RegularEdge]) -> Option<Vec<usize>> {
    if relations.is_empty() {
        return Some(Vec::new());
    }

    let mut relations = relations.to_vec();
    relations.sort_unstable_by_key(|relation| relation.node);
    let positions = relations
        .iter()
        .enumerate()
        .map(|(position, relation)| (relation.node, position))
        .collect::<HashMap<_, _>>();
    let dense_edges = edges
        .iter()
        .filter_map(|edge| {
            let left = *positions.get(&edge.left)?;
            let right = *positions.get(&edge.right)?;
            (left != right).then_some(DenseEdge {
                left,
                right,
                selectivity: non_negative_or_neutral(edge.selectivity),
            })
        })
        .collect::<Vec<_>>();
    let tree = minimum_spanning_tree(&relations, dense_edges)?;

    (0..relations.len())
        .map(|root| rooted_order(root, &relations, &tree))
        .min_by(|left, right| {
            left.cost
                .total_cmp(&right.cost)
                .then_with(|| left.nodes.cmp(&right.nodes))
        })
        .map(|order| order.nodes)
}

#[derive(Debug, Clone, Copy)]
struct DenseEdge {
    left: usize,
    right: usize,
    selectivity: f64,
}

#[derive(Debug, Clone, Copy)]
struct TreeEdge {
    neighbor: usize,
    selectivity: f64,
}

fn minimum_spanning_tree(
    relations: &[Relation],
    mut edges: Vec<DenseEdge>,
) -> Option<Vec<Vec<TreeEdge>>> {
    edges.sort_by(|left, right| {
        left.selectivity
            .total_cmp(&right.selectivity)
            .then_with(|| edge_key(*left, relations).cmp(&edge_key(*right, relations)))
    });

    let mut components = DisjointSet::new(relations.len());
    let mut tree = vec![Vec::new(); relations.len()];
    let mut selected = 0;
    for edge in edges {
        if !components.union(edge.left, edge.right) {
            continue;
        }
        tree[edge.left].push(TreeEdge {
            neighbor: edge.right,
            selectivity: edge.selectivity,
        });
        tree[edge.right].push(TreeEdge {
            neighbor: edge.left,
            selectivity: edge.selectivity,
        });
        selected += 1;
        if selected + 1 == relations.len() {
            break;
        }
    }

    (selected + 1 == relations.len()).then_some(tree)
}

fn edge_key(edge: DenseEdge, relations: &[Relation]) -> (usize, usize) {
    let left = relations[edge.left].node;
    let right = relations[edge.right].node;
    (left.min(right), left.max(right))
}

struct RootedOrder {
    nodes: Vec<usize>,
    cost: f64,
}

fn rooted_order(root: usize, relations: &[Relation], tree: &[Vec<TreeEdge>]) -> RootedOrder {
    let chain = build_chain(root, None, 1.0, relations, tree);
    let cost = chain
        .iter()
        .map(|compound| compound.summary)
        .reduce(COutSummary::then)
        .expect("a rooted order has at least one relation")
        .cost();
    let nodes = chain
        .into_iter()
        .flat_map(|compound| compound.nodes)
        .collect();
    RootedOrder { nodes, cost }
}

/// Converts the subtree rooted at `node` into a chain.
///
/// Each returned child chain is normalized by its caller before independent chains are merged.
/// Consequently the chain headed by the chosen global root is never normalized past that root,
/// preserving the fixed-start precedence constraint.
fn build_chain(
    node: usize,
    parent: Option<usize>,
    incoming_selectivity: f64,
    relations: &[Relation],
    tree: &[Vec<TreeEdge>],
) -> Vec<Compound> {
    let child_chains = tree[node]
        .iter()
        .filter(|edge| Some(edge.neighbor) != parent)
        .map(|edge| {
            normalize(build_chain(
                edge.neighbor,
                Some(node),
                edge.selectivity,
                relations,
                tree,
            ))
        })
        .collect::<Vec<_>>();

    let head = match parent {
        None => Compound::root(relations[node].node, relations[node].cardinality),
        Some(_) => Compound::relation(
            relations[node].node,
            relations[node].cardinality,
            incoming_selectivity,
        ),
    };
    std::iter::once(head)
        .chain(merge_by_rank(child_chains))
        .collect()
}

#[derive(Debug, Clone)]
struct Compound {
    nodes: Vec<usize>,
    summary: COutSummary,
}

impl Compound {
    fn root(node: usize, cardinality: f64) -> Self {
        Self {
            nodes: vec![node],
            summary: COutSummary::root(cardinality),
        }
    }

    fn relation(node: usize, cardinality: f64, selectivity: f64) -> Self {
        Self {
            nodes: vec![node],
            summary: COutSummary::relation(cardinality, selectivity),
        }
    }

    fn then(mut self, next: Self) -> Self {
        self.nodes.extend(next.nodes);
        self.summary = self.summary.then(next.summary);
        self
    }

    fn cmp_rank(&self, other: &Self) -> std::cmp::Ordering {
        self.summary
            .rank_cmp(other.summary)
            .then_with(|| self.nodes.cmp(&other.nodes))
    }
}

/// Collapses every rank inversion while preserving the chain's precedence.
///
/// The stack formulation is the pool-adjacent-violators form of IKKBZ normalization: after a
/// merge, the new compound is immediately compared with its predecessor, so the result is a
/// nondecreasing chain of maximal compounds in one pass.
fn normalize(chain: Vec<Compound>) -> Vec<Compound> {
    chain.into_iter().fold(Vec::new(), |mut normalized, next| {
        normalized.push(next);
        while normalized.len() >= 2 {
            let split = normalized.len() - 1;
            if !normalized[split - 1]
                .summary
                .rank_cmp(normalized[split].summary)
                .is_gt()
            {
                break;
            }
            let right = normalized.pop().expect("right compound exists");
            let left = normalized.pop().expect("left compound exists");
            normalized.push(left.then(right));
        }
        normalized
    })
}

/// Stable k-way merge of already normalized child chains.
fn merge_by_rank(chains: Vec<Vec<Compound>>) -> Vec<Compound> {
    let total_len = chains.iter().map(Vec::len).sum();
    let mut positions = vec![0; chains.len()];
    let mut merged = Vec::with_capacity(total_len);

    while merged.len() < total_len {
        let next_chain = chains
            .iter()
            .enumerate()
            .filter(|(chain, compounds)| positions[*chain] < compounds.len())
            .min_by(|(left_index, left), (right_index, right)| {
                left[positions[*left_index]]
                    .cmp_rank(&right[positions[*right_index]])
                    .then_with(|| left_index.cmp(right_index))
            })
            .map(|(chain, _)| chain)
            .expect("an incomplete merge has a non-empty chain");
        let position = positions[next_chain];
        merged.push(chains[next_chain][position].clone());
        positions[next_chain] += 1;
    }
    merged
}

/// Keeps useful extended-cardinality values while neutralizing invalid estimates.
///
/// Mapping NaN or a negative value to zero would make a corrupt estimate look maximally
/// selective and could force that edge into the spanning tree. One is the multiplicative neutral
/// value and therefore the conservative deterministic fallback.
fn non_negative_or_neutral(value: f64) -> f64 {
    if value.is_nan() || value.is_sign_negative() {
        1.0
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relations(cardinalities: &[f64]) -> Vec<Relation> {
        cardinalities
            .iter()
            .copied()
            .enumerate()
            .map(|(node, cardinality)| Relation { node, cardinality })
            .collect()
    }

    fn edges(edges: &[(usize, usize, f64)]) -> Vec<RegularEdge> {
        edges
            .iter()
            .copied()
            .map(|(left, right, selectivity)| RegularEdge {
                left,
                right,
                selectivity,
            })
            .collect()
    }

    fn order_cost(order: &[usize], relations: &[Relation], edges: &[RegularEdge]) -> Option<f64> {
        let cardinality = relations
            .iter()
            .map(|relation| (relation.node, relation.cardinality))
            .collect::<HashMap<_, _>>();
        let mut joined = vec![order[0]];
        let mut rows = cardinality[&order[0]];
        let mut cost = 0.0;
        for &node in &order[1..] {
            let edge = edges.iter().find(|edge| {
                (edge.left == node && joined.contains(&edge.right))
                    || (edge.right == node && joined.contains(&edge.left))
            })?;
            rows *= cardinality[&node] * edge.selectivity;
            cost += rows;
            joined.push(node);
        }
        Some(cost)
    }

    fn permutations(values: &mut [usize], start: usize, visit: &mut impl FnMut(&[usize])) {
        if start == values.len() {
            visit(values);
            return;
        }
        for next in start..values.len() {
            values.swap(start, next);
            permutations(values, start + 1, visit);
            values.swap(start, next);
        }
    }

    fn exhaustive_best(relations: &[Relation], edges: &[RegularEdge]) -> f64 {
        let mut order = relations
            .iter()
            .map(|relation| relation.node)
            .collect::<Vec<_>>();
        let mut best = f64::INFINITY;
        permutations(&mut order, 0, &mut |candidate| {
            if let Some(cost) = order_cost(candidate, relations, edges) {
                best = best.min(cost);
            }
        });
        best
    }

    fn tree_from_pruefer(code: &[usize], node_count: usize) -> Vec<(usize, usize)> {
        let mut degrees = vec![1; node_count];
        for &node in code {
            degrees[node] += 1;
        }
        let mut result = Vec::with_capacity(node_count - 1);
        for &node in code {
            let leaf = degrees
                .iter()
                .position(|degree| *degree == 1)
                .expect("every Prüfer step has a leaf");
            result.push((leaf, node));
            degrees[leaf] -= 1;
            degrees[node] -= 1;
        }
        let remaining = degrees
            .iter()
            .enumerate()
            .filter_map(|(node, degree)| (*degree == 1).then_some(node))
            .collect::<Vec<_>>();
        result.push((remaining[0], remaining[1]));
        result
    }

    #[test]
    fn normalization_collapses_rank_conflicts_into_compounds() {
        let normalized = normalize(vec![
            Compound::relation(0, 10.0, 1.0),
            Compound::relation(1, 1.0, 0.5),
            Compound::relation(2, 2.0, 1.0),
        ]);

        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0].nodes, vec![0, 1]);
        assert_eq!(normalized[1].nodes, vec![2]);
        assert!(!normalized[0].cmp_rank(&normalized[1]).is_gt());
    }

    #[test]
    fn minimum_spanning_tree_prefers_selective_edges_and_breaks_ties_stably() {
        let relations = relations(&[1.0; 4]);
        let positions = (0..4)
            .map(|node| (relations[node].node, node))
            .collect::<HashMap<_, _>>();
        let dense = edges(&[
            (0, 1, 0.9),
            (1, 2, 0.1),
            (0, 2, 0.2),
            (2, 3, 0.3),
            (0, 3, 0.8),
            // A parallel edge with the same weight loses the deterministic endpoint tie-break.
            (1, 3, 0.3),
        ])
        .into_iter()
        .map(|edge| DenseEdge {
            left: positions[&edge.left],
            right: positions[&edge.right],
            selectivity: edge.selectivity,
        })
        .collect();

        let tree = minimum_spanning_tree(&relations, dense).expect("the graph is connected");
        let mut selected = tree
            .iter()
            .enumerate()
            .flat_map(|(left, edges)| {
                edges
                    .iter()
                    .filter(move |edge| left < edge.neighbor)
                    .map(move |edge| (left, edge.neighbor))
            })
            .collect::<Vec<_>>();
        selected.sort_unstable();

        assert_eq!(selected, vec![(0, 2), (1, 2), (1, 3)]);
    }

    #[test]
    fn ikkbz_matches_exhaustive_left_deep_oracle_on_small_trees() {
        let cases = [
            (
                vec![100.0, 4.0, 50.0, 2.0, 20.0, 8.0],
                vec![
                    (0, 1, 0.5),
                    (0, 2, 0.125),
                    (2, 3, 0.25),
                    (2, 4, 0.5),
                    (4, 5, 0.125),
                ],
            ),
            (
                vec![2.0, 64.0, 4.0, 32.0, 8.0, 16.0],
                vec![
                    (0, 1, 0.25),
                    (1, 2, 0.5),
                    (1, 3, 0.125),
                    (3, 4, 0.25),
                    (3, 5, 0.5),
                ],
            ),
            (
                vec![64.0, 2.0, 32.0, 4.0, 16.0, 8.0],
                vec![
                    (0, 1, 0.5),
                    (1, 2, 0.25),
                    (2, 3, 0.125),
                    (3, 4, 0.5),
                    (4, 5, 0.25),
                ],
            ),
        ];

        for (cardinalities, edge_data) in cases {
            let relations = relations(&cardinalities);
            let edges = edges(&edge_data);
            let order = linearize(&relations, &edges).expect("a tree is connected");
            let actual = order_cost(&order, &relations, &edges).expect("IKKBZ order is legal");
            let expected = exhaustive_best(&relations, &edges);

            assert_eq!(actual, expected, "suboptimal IKKBZ order: {order:?}");
        }
    }

    #[test]
    fn root_scan_cardinality_does_not_distort_c_out_ordering() {
        let relations = relations(&[1_000.0, 1_000.0, 1.0]);
        let edges = edges(&[(0, 1, 0.0001), (1, 2, 0.101)]);

        // The first join outputs are 100 rows for (0,1) and 101 rows for (1,2). Charging the
        // chosen base root would incorrectly favor node 2 merely because it has one row.
        let order = linearize(&relations, &edges).expect("the chain is connected");

        assert_eq!(order, vec![0, 1, 2]);
        let actual = order_cost(&order, &relations, &edges).unwrap();
        assert!((actual - 110.1).abs() < 1e-10);
        assert_eq!(actual, exhaustive_best(&relations, &edges));
    }

    #[test]
    fn ikkbz_matches_oracle_for_every_labeled_five_node_tree() {
        const NODE_COUNT: usize = 5;
        const SELECTIVITIES: [f64; 5] = [0.03125, 0.0625, 0.125, 0.25, 0.5];
        let mut code = [0; NODE_COUNT - 2];

        for tree_index in 0..NODE_COUNT.pow((NODE_COUNT - 2) as u32) {
            let mut encoded = tree_index;
            for digit in &mut code {
                *digit = encoded % NODE_COUNT;
                encoded /= NODE_COUNT;
            }
            let cardinalities = (0..NODE_COUNT)
                .map(|node| 2_f64.powi(((node + tree_index) % NODE_COUNT + 1) as i32))
                .collect::<Vec<_>>();
            let relations = relations(&cardinalities);
            let edge_data = tree_from_pruefer(&code, NODE_COUNT)
                .into_iter()
                .enumerate()
                .map(|(edge, (left, right))| {
                    (
                        left,
                        right,
                        SELECTIVITIES[(tree_index + edge * 3) % SELECTIVITIES.len()],
                    )
                })
                .collect::<Vec<_>>();
            let edges = edges(&edge_data);
            let order = linearize(&relations, &edges).expect("a labeled tree is connected");
            let actual = order_cost(&order, &relations, &edges).expect("IKKBZ order is legal");
            let expected = exhaustive_best(&relations, &edges);

            assert_eq!(
                actual, expected,
                "suboptimal IKKBZ order for Prüfer code {code:?}: {order:?}"
            );
        }
    }

    #[test]
    fn cyclic_graph_linearization_keeps_every_prefix_connected() {
        let relations = relations(&[100.0, 10.0, 50.0, 5.0, 20.0]);
        let edges = edges(&[
            (0, 1, 0.1),
            (1, 2, 0.2),
            (2, 0, 0.05),
            (2, 3, 0.4),
            (3, 4, 0.25),
            (4, 1, 0.3),
        ]);
        let order = linearize(&relations, &edges).expect("the graph is connected");

        for prefix_end in 1..order.len() {
            let node = order[prefix_end];
            assert!(edges.iter().any(|edge| {
                (edge.left == node && order[..prefix_end].contains(&edge.right))
                    || (edge.right == node && order[..prefix_end].contains(&edge.left))
            }));
        }
    }

    #[test]
    fn disconnected_graph_has_no_ikkbz_order() {
        assert_eq!(
            linearize(&relations(&[1.0, 1.0, 1.0]), &edges(&[(0, 1, 1.0)])),
            None
        );
    }
}
