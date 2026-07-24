//! Pure query-hypergraph operations shared by join enumerators.

use std::collections::HashSet;

use crate::hypergraph::{
    HyperedgeJoinType, NodeSet, QueryHypergraph, nodeset_min, nodeset_singleton,
};
use crate::relation_set::NonEmptySubsets;

/// Indexed view of join connectivity.
///
/// Each relation stores the hyperedges that touch it. Sparse large graphs can therefore discover
/// neighbors by probing the current component instead of repeatedly scanning every query edge.
pub(super) struct JoinGraph<'a> {
    hypergraph: &'a QueryHypergraph,
    incident_edges: Vec<Vec<usize>>,
}

/// Hyperedges crossing one active component's boundary.
///
/// The indices are sorted and unique. GOO moves these sets out of the two consumed children when
/// it creates their parent, so the frontier never retains boundary storage for inactive
/// components.
pub(super) struct EdgeBoundary {
    edge_indices: Vec<usize>,
}

impl EdgeBoundary {
    pub(super) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.edge_indices.iter().copied()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.edge_indices.len()
    }
}

impl<'a> JoinGraph<'a> {
    pub(super) fn new(hypergraph: &'a QueryHypergraph) -> Self {
        let mut incident_edges = vec![Vec::new(); hypergraph.nodes.len()];
        for (edge_index, edge) in hypergraph.edges.iter().enumerate() {
            for node in edge.left.iter().chain(edge.right.iter()) {
                incident_edges[node].push(edge_index);
            }
        }
        Self {
            hypergraph,
            incident_edges,
        }
    }

    /// DPhyp neighborhood `N(S, X)`, including canonical representatives for hyperedges.
    pub(super) fn neighborhood(&self, set: &NodeSet, excluded: &NodeSet) -> NodeSet {
        self.neighborhood_within(set, excluded, &NodeSet::all(self.hypergraph.nodes.len()))
    }

    /// DPhyp neighborhood restricted to an induced relation subset.
    pub(super) fn neighborhood_within(
        &self,
        set: &NodeSet,
        excluded: &NodeSet,
        allowed: &NodeSet,
    ) -> NodeSet {
        self.edge_indices_touching(set)
            .fold(NodeSet::EMPTY, |mut result, edge_index| {
                let edge = &self.hypergraph.edges[edge_index];
                if edge.left.is_subset(allowed)
                    && edge.right.is_subset(allowed)
                    && edge.left.is_subset(set)
                    && edge.right.is_disjoint(set)
                    && edge.right.is_disjoint(excluded)
                {
                    result |= &nodeset_singleton(nodeset_min(&edge.right));
                }
                if edge.left.is_subset(allowed)
                    && edge.right.is_subset(allowed)
                    && edge.right.is_subset(set)
                    && edge.left.is_disjoint(set)
                    && edge.left.is_disjoint(excluded)
                {
                    result |= &nodeset_singleton(nodeset_min(&edge.left));
                }
                result
            })
    }

    /// Returns true if a hyperedge is applicable between two disjoint components.
    pub(super) fn connects(&self, left: &NodeSet, right: &NodeSet) -> bool {
        let probe = if left.len() <= right.len() {
            left
        } else {
            right
        };
        self.edge_indices_touching(probe).any(|edge_index| {
            let edge = &self.hypergraph.edges[edge_index];
            (edge.left.is_subset(left) && edge.right.is_subset(right))
                || (edge.left.is_subset(right) && edge.right.is_subset(left))
        })
    }

    /// Sorted hypergraph edges that become applicable between two components.
    pub(super) fn connecting_edge_indices(&self, left: &NodeSet, right: &NodeSet) -> Vec<usize> {
        let probe = if left.len() <= right.len() {
            left
        } else {
            right
        };
        let mut indices = self.edge_indices_touching(probe).collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        indices.retain(|&edge_index| {
            let edge = &self.hypergraph.edges[edge_index];
            (edge.left.is_subset(left) && edge.right.is_subset(right))
                || (edge.left.is_subset(right) && edge.right.is_subset(left))
        });
        indices
    }

    /// The TES endpoints of one indexed hyperedge.
    pub(super) fn edge_endpoints(&self, edge_index: usize) -> (&NodeSet, &NodeSet) {
        let edge = &self.hypergraph.edges[edge_index];
        (&edge.left, &edge.right)
    }

    /// Number of indexed hyperedges.
    pub(super) fn edge_count(&self) -> usize {
        self.hypergraph.edges.len()
    }

    /// Boundary edges touching a singleton component.
    pub(super) fn singleton_boundary(&self, node: usize) -> EdgeBoundary {
        let mut edge_indices = self.incident_edges[node].clone();
        edge_indices.dedup();
        EdgeBoundary { edge_indices }
    }

    /// Moves two child boundaries into their merged component.
    ///
    /// Edges whose complete TES is now internal can never connect this component to a future
    /// neighbor and are discarded immediately. Reusing the larger child allocation keeps peak
    /// storage close to the active frontier's total incidence count.
    pub(super) fn merge_boundaries(
        &self,
        left: EdgeBoundary,
        right: EdgeBoundary,
        merged: &NodeSet,
    ) -> EdgeBoundary {
        let (mut edge_indices, mut other) =
            if left.edge_indices.capacity() >= right.edge_indices.capacity() {
                (left.edge_indices, right.edge_indices)
            } else {
                (right.edge_indices, left.edge_indices)
            };
        edge_indices.append(&mut other);
        edge_indices.sort_unstable();
        edge_indices.dedup();
        edge_indices.retain(|&edge_index| {
            let edge = &self.hypergraph.edges[edge_index];
            !(edge.left.is_subset(merged) && edge.right.is_subset(merged))
        });
        EdgeBoundary { edge_indices }
    }

    /// Whether classic IKKBZ's ordinary inner-join graph assumptions hold.
    ///
    /// Singleton endpoints alone are insufficient: a simple outer or semi join is still outside
    /// the ASI proof even though its TES happens to look like a regular edge.
    pub(super) fn supports_ikkbz_linearization(&self) -> bool {
        self.hypergraph.edges.iter().all(|edge| {
            edge.join_type == HyperedgeJoinType::Inner
                && edge.left.len() == 1
                && edge.right.len() == 1
        })
    }

    fn edge_indices_touching<'set>(
        &'set self,
        set: &'set NodeSet,
    ) -> impl Iterator<Item = usize> + 'set {
        set.iter()
            .flat_map(|node| self.incident_edges[node].iter().copied())
    }

    /// Counts DPhyp table states, stopping immediately after `budget` is exceeded.
    ///
    /// For ordinary graphs this is Figure 3 from Neumann and Radke (SIGMOD 2018). Genuine
    /// hyperedges need a stricter treatment: adding the canonical representative of one TES side
    /// can temporarily produce a set that is not itself joinable. Counting those partial
    /// expansions would overestimate the DPhyp table. The hypergraph path instead computes the
    /// exact closure of singleton states under applicable csg-cmp joins.
    pub(super) fn count_connected_subgraphs(&self, budget: usize) -> BoundedCount {
        if self.hypergraph.nodes.len() > budget {
            return BoundedCount::Exceeded;
        }
        if self
            .hypergraph
            .edges
            .iter()
            .any(|edge| edge.left.len() != 1 || edge.right.len() != 1)
        {
            return self.count_hypergraph_states(budget);
        }

        let mut count = 0;
        for node in 0..self.hypergraph.nodes.len() {
            count += 1;
            if count > budget {
                return BoundedCount::Exceeded;
            }
            if self.count_from_seed(
                nodeset_singleton(node),
                NodeSet::prefix_inclusive(node),
                &mut count,
                budget,
            ) {
                return BoundedCount::Exceeded;
            }
        }
        BoundedCount::Within(count)
    }

    /// Computes the exact set of DPhyp states for a genuine hypergraph.
    ///
    /// A non-singleton state exists exactly when it is the union of two smaller, disjoint states
    /// connected by an applicable hyperedge. Processing states in insertion order tests every pair
    /// once: when the later state is visited, the earlier one is already present. This is both
    /// sound (every inserted state has a valid final join) and complete (the two children of every
    /// possible final join are inserted first by induction on state size).
    ///
    /// The number of retained states is bounded by `budget + 1`, and the worklist is iterative, so
    /// this path remains stack-safe. Hypergraphs are rare and use a 10,000-state budget by default;
    /// ordinary graphs retain the more efficient Figure-3 traversal above.
    fn count_hypergraph_states(&self, budget: usize) -> BoundedCount {
        let mut states = (0..self.hypergraph.nodes.len())
            .map(nodeset_singleton)
            .collect::<Vec<_>>();
        let mut known = states.iter().cloned().collect::<HashSet<_>>();
        let mut next = 0;

        while next < states.len() {
            let current = states[next].clone();
            for other_index in 0..next {
                let candidate = {
                    let other = &states[other_index];
                    if !current.is_disjoint(other) || !self.connects(&current, other) {
                        continue;
                    }
                    &current | other
                };

                if known.insert(candidate.clone()) {
                    states.push(candidate);
                    if known.len() > budget {
                        return BoundedCount::Exceeded;
                    }
                }
            }
            next += 1;
        }

        BoundedCount::Within(known.len())
    }

    /// Iterative form of Figure 3's recursive expansion.
    ///
    /// A chain with thousands of relations can have a cheap bounded count but a recursion depth
    /// large enough to exhaust the Rust stack. Explicit frames preserve the paper's DFS order and
    /// early termination without imposing a relation-count limit.
    fn count_from_seed(
        &self,
        set: NodeSet,
        excluded: NodeSet,
        count: &mut usize,
        budget: usize,
    ) -> bool {
        let mut stack = vec![self.count_frame(set, excluded)];
        while let Some(frame) = stack.last_mut() {
            let next = frame
                .extensions
                .next()
                .map(|extension| (&frame.set | &extension, frame.child_excluded.clone()));
            let Some((set, excluded)) = next else {
                stack.pop();
                continue;
            };

            *count += 1;
            if *count > budget {
                return true;
            }
            stack.push(self.count_frame(set, excluded));
        }
        false
    }

    fn count_frame(&self, set: NodeSet, excluded: NodeSet) -> CountFrame {
        let neighborhood = self.neighborhood(&set, &excluded);
        CountFrame {
            set,
            child_excluded: &excluded | &neighborhood,
            extensions: neighborhood.non_empty_subsets(),
        }
    }
}

struct CountFrame {
    set: NodeSet,
    child_excluded: NodeSet,
    extensions: NonEmptySubsets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundedCount {
    Within(usize),
    Exceeded,
}
