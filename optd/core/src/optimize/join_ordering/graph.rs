//! Pure query-hypergraph operations shared by join enumerators.

use crate::hypergraph::{NodeSet, QueryHypergraph, nodeset_min, nodeset_singleton};

/// A borrowed, allocation-free view of join connectivity.
#[derive(Clone, Copy)]
pub(super) struct JoinGraph<'a> {
    hypergraph: &'a QueryHypergraph,
}

impl<'a> JoinGraph<'a> {
    pub(super) fn new(hypergraph: &'a QueryHypergraph) -> Self {
        Self { hypergraph }
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
        self.hypergraph
            .edges
            .iter()
            .fold(NodeSet::EMPTY, |mut result, edge| {
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
        self.hypergraph.edges.iter().any(|edge| {
            (edge.left.is_subset(left) && edge.right.is_subset(right))
                || (edge.left.is_subset(right) && edge.right.is_subset(left))
        })
    }

    /// Whether any edge requires more than one relation on either endpoint.
    pub(super) fn has_hyperedges(&self) -> bool {
        self.hypergraph
            .edges
            .iter()
            .any(|edge| edge.left.len() > 1 || edge.right.len() > 1)
    }

    /// Counts connected subgraphs, stopping immediately after `budget` is exceeded.
    ///
    /// This is Figure 3 from Neumann and Radke (SIGMOD 2018), generalized to DPhyp
    /// neighborhoods. The bounded count predicts exact-DP memory without constructing plans.
    pub(super) fn count_connected_subgraphs(&self, budget: usize) -> BoundedCount {
        let mut count = 0;
        for node in 0..self.hypergraph.nodes.len() {
            count += 1;
            if count > budget {
                return BoundedCount::Exceeded;
            }
            if self.count_rec(
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

    fn count_rec(&self, set: NodeSet, excluded: NodeSet, count: &mut usize, budget: usize) -> bool {
        let neighborhood = self.neighborhood(&set, &excluded);
        for extension in neighborhood.non_empty_subsets() {
            *count += 1;
            if *count > budget {
                return true;
            }
            if self.count_rec(&set | &extension, &excluded | &neighborhood, count, budget) {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundedCount {
    Within(usize),
    Exceeded,
}
