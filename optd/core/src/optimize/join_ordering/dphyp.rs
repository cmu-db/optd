//! Complete DPhyp enumeration and DP state management.

use std::collections::HashMap;

use super::OptimizeResult;
use super::candidate::JoinSearch;
use super::graph::JoinGraph;
use super::plan::PlanState;
use crate::analysis::connecting_edge_indices;
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, nodeset_min, nodeset_singleton};

// ---------------------------------------------------------------------------
// DPhyp
// ---------------------------------------------------------------------------

/// Complete dynamic programming over connected-subgraph/complement pairs.
///
/// The DP table contains the cheapest known plan for every enumerated connected [`NodeSet`].
/// Enumeration follows DPhyp's two mutually recursive phases:
///
/// 1. `enumerate_csg_rec` grows a connected subgraph `S1` from a canonical seed.
/// 2. `enumerate_cmp_rec` grows connected complements `S2` and emits `(S1, S2)` whenever both
///    partial plans exist and an applicable hyperedge connects them.
///
/// `allowed` restricts both phases to an induced subgraph. The GOO/DP implementation uses this to
/// solve bounded subtrees exactly without copying or renumbering the original hypergraph.
pub(super) struct DPhyp<'search, 'ctx, M: CostModel> {
    /// Shared candidate evaluator, recipe arena, IR, and analyses for this join group.
    search: &'search mut JoinSearch<'ctx, M>,
    /// DP table: NodeSet → best known plan for that subset.
    dp: HashMap<NodeSet, PlanState<M::Cost>>,
    /// Induced node set visible to the current solve.
    allowed: NodeSet,
}

impl<'search, 'ctx, M: CostModel> DPhyp<'search, 'ctx, M> {
    /// Creates an exact solver over the complete hypergraph.
    pub(super) fn new(search: &'search mut JoinSearch<'ctx, M>) -> Self {
        let relation_count = search.hypergraph().nodes.len();
        Self {
            search,
            dp: HashMap::new(),
            allowed: NodeSet::all(relation_count),
        }
    }

    /// Solves the full join group and returns its cheapest complete plan.
    pub(super) fn solve(&mut self) -> OptimizeResult<Option<PlanState<M::Cost>>> {
        let all = NodeSet::all(self.search.hypergraph().nodes.len());
        self.solve_subset(&all)
    }

    /// Solves the subgraph induced by `allowed`.
    ///
    /// Singleton plans are initialized from existing hypergraph-node roots. The DP table is then
    /// rebuilt from scratch, allowing one solver value to be reused safely for distinct induced
    /// subproblems. `None` is returned for an empty set or when no complete connected plan can be
    /// formed.
    pub(super) fn solve_subset(
        &mut self,
        allowed: &NodeSet,
    ) -> OptimizeResult<Option<PlanState<M::Cost>>> {
        if allowed.is_empty() {
            return Ok(None);
        }
        self.dp.clear();
        self.allowed = allowed.clone();
        // Every base relation/non-join subtree is already a valid one-node plan.
        for i in allowed.iter() {
            let s = nodeset_singleton(i);
            self.dp.insert(s, self.search.leaf(i)?);
        }

        // Descending seeds plus B_min form DPhyp's canonical enumeration order.
        for v in allowed.iter().collect::<Vec<_>>().into_iter().rev() {
            let sv = nodeset_singleton(v);
            self.emit_csg(sv.clone())?;
            self.enumerate_csg_rec(sv, NodeSet::prefix_inclusive(v))?;
        }

        Ok(self.dp.get(allowed).cloned())
    }

    /// Recursively grows canonical connected subgraphs from `s1`.
    ///
    /// The first pass emits all already-solvable extensions before the second pass recurses. This
    /// preserves the DP dependency order required by later complement enumeration.
    fn enumerate_csg_rec(&mut self, s1: NodeSet, x: NodeSet) -> OptimizeResult<()> {
        let nbrs = self.neighborhood(&s1, &x);
        // Collect non-empty subsets of nbrs.
        let subsets = non_empty_subsets(nbrs.clone());
        // First pass: emit csgs.
        for n_sub in &subsets {
            let candidate = &s1 | n_sub;
            if self.dp.contains_key(&candidate) {
                self.emit_csg(candidate)?;
            }
        }
        // Second pass: recurse.
        for n_sub in &subsets {
            self.enumerate_csg_rec(&s1 | n_sub, &x | &nbrs)?;
        }
        Ok(())
    }

    /// Enumerates complements for one connected subgraph.
    fn emit_csg(&mut self, s1: NodeSet) -> OptimizeResult<()> {
        let x = &s1 | &self.b_min(&s1);
        let nbrs = self.neighborhood(&s1, &x);
        // Iterate neighbors in descending order.
        for node in nbrs.iter().collect::<Vec<_>>().into_iter().rev() {
            let s2 = nodeset_singleton(node);
            if self.has_edge(&s1, &s2) {
                self.emit_csg_cmp(&s1, &s2)?;
            }
            self.enumerate_cmp_rec(s1.clone(), s2, x.clone())?;
        }
        Ok(())
    }

    /// Recursively grows a connected complement `s2` for the fixed `s1`.
    fn enumerate_cmp_rec(&mut self, s1: NodeSet, s2: NodeSet, x: NodeSet) -> OptimizeResult<()> {
        let nbrs = self.neighborhood(&s2, &x);
        let subsets = non_empty_subsets(nbrs.clone());
        for n_sub in &subsets {
            let candidate = &s2 | n_sub;
            if self.dp.contains_key(&candidate) && self.has_edge(&s1, &candidate) {
                self.emit_csg_cmp(&s1, &candidate)?;
            }
        }
        for n_sub in &subsets {
            self.enumerate_cmp_rec(s1.clone(), &s2 | n_sub, &x | &nbrs)?;
        }
        Ok(())
    }

    /// Costs and records one applicable csg-cmp pair.
    ///
    /// Multiple hyperedges may become applicable at the same split. Their predicates are passed
    /// together to candidate materialization, ensuring every predicate is installed at the first
    /// operator whose inputs make both test endpoints available.
    fn emit_csg_cmp(&mut self, s1: &NodeSet, s2: &NodeSet) -> OptimizeResult<()> {
        let Some(left) = self.dp.get(s1).cloned() else {
            return Ok(());
        };
        let Some(right) = self.dp.get(s2).cloned() else {
            return Ok(());
        };

        let edge_indices = connecting_edge_indices(s1, s2, self.search.hypergraph());
        if edge_indices.is_empty() {
            return Ok(());
        }

        let Some(candidate) =
            self.search
                .best_join_candidate(s1, &left, s2, &right, edge_indices)?
        else {
            return Ok(());
        };
        let combined = s1 | s2;

        let better = self
            .dp
            .get(&combined)
            .is_none_or(|existing| self.search.is_better(candidate.cost(), &existing.cost));

        if better {
            let state = self.search.commit(candidate);
            self.dp.insert(combined, state);
        }
        Ok(())
    }

    /// Returns DPhyp's neighborhood `N(s, x)` inside the current induced subgraph.
    fn neighborhood(&self, s: &NodeSet, x: &NodeSet) -> NodeSet {
        JoinGraph::new(self.search.hypergraph()).neighborhood_within(s, x, &self.allowed)
    }

    /// Returns whether an applicable hyperedge connects `s1` and `s2`.
    fn has_edge(&self, s1: &NodeSet, s2: &NodeSet) -> bool {
        JoinGraph::new(self.search.hypergraph()).connects(s1, s2)
    }

    /// Returns `B_min(s)`: all nodes with index ≤ `min(s)`.
    ///
    /// Adding this prefix to the exclusion set selects one canonical seed for every connected
    /// subgraph and prevents duplicate csg-cmp emission.
    fn b_min(&self, s: &NodeSet) -> NodeSet {
        NodeSet::prefix_inclusive(nodeset_min(s))
    }
}

/// Enumerates all non-empty subsets of `s` in ascending order.
fn non_empty_subsets(s: NodeSet) -> Vec<NodeSet> {
    s.non_empty_subsets().collect()
}

#[cfg(test)]
pub(super) fn all_nodes_mask(n: usize) -> NodeSet {
    NodeSet::all(n)
}
