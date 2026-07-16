//! Complete DPhyp enumeration and DP state management.

use std::collections::HashMap;

use super::{OptimizeResult, candidate::best_join_candidate, graph::JoinGraph};
use crate::analysis::connecting_edge_indices;
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, QueryHypergraph, nodeset_min, nodeset_singleton};
use crate::{AnalysisContext, Operator, QueryContext};

// ---------------------------------------------------------------------------
// JoinTree: the output of DPhyp
// ---------------------------------------------------------------------------

#[cfg(test)]
#[derive(Clone)]
/// Lightweight witness tree retained only in tests.
///
/// Production states keep the winning operator root directly. Avoiding this parallel tree in
/// release builds matters for large DP tables because cloning it would make candidate comparison
/// proportional to subtree size.
pub(super) enum JoinTree {
    Leaf(usize), // node index
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
    },
}

#[cfg(test)]
impl JoinTree {
    pub(super) fn leaf_count(&self) -> usize {
        match self {
            JoinTree::Leaf(_) => 1,
            JoinTree::Join { left, right, .. } => left.leaf_count() + right.leaf_count(),
        }
    }

    pub(super) fn leaf_set(&self) -> NodeSet {
        match self {
            JoinTree::Leaf(nid) => nodeset_singleton(*nid),
            JoinTree::Join { left, right, .. } => &left.leaf_set() | &right.leaf_set(),
        }
    }

    pub(super) fn has_join_with_leaves(&self, leaves: &NodeSet) -> bool {
        match self {
            JoinTree::Leaf(_) => false,
            JoinTree::Join { left, right, .. } => {
                self.leaf_set() == *leaves
                    || left.has_join_with_leaves(leaves)
                    || right.has_join_with_leaves(leaves)
            }
        }
    }
}

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
pub(super) struct DPhyp<'a, M: CostModel> {
    /// Arena and IR payloads used to materialize candidate joins.
    ctx: &'a mut QueryContext,
    /// Demand-driven analyses used by the cost model.
    analyses: &'a mut AnalysisContext,
    /// Immutable join group being enumerated.
    hg: &'a QueryHypergraph,
    /// Cost algebra and operator-local costing implementation.
    cost_model: &'a M,
    /// DP table: NodeSet → best known plan for that subset.
    dp: HashMap<NodeSet, PlanState<M::Cost>>,
    /// Induced node set visible to the current solve.
    allowed: NodeSet,
}

#[derive(Clone)]
/// Winning physical/logical candidate for one relation set.
///
/// Child costs are composed through [`CostModel::total_cost_from_children`], so replacing a DP
/// entry does not require walking the already-costed subtrees again.
pub(super) struct PlanState<C> {
    /// Root operator of the materialized candidate.
    pub(super) root: Operator,
    /// Total cost of the complete subtree rooted at `root`.
    pub(super) cost: C,
    #[cfg(test)]
    pub(super) tree: JoinTree,
}

impl<'a, M: CostModel> DPhyp<'a, M> {
    /// Creates an exact solver over the complete hypergraph.
    pub(super) fn new(
        ctx: &'a mut QueryContext,
        analyses: &'a mut AnalysisContext,
        hg: &'a QueryHypergraph,
        cost_model: &'a M,
    ) -> Self {
        Self {
            ctx,
            analyses,
            hg,
            cost_model,
            dp: HashMap::new(),
            allowed: NodeSet::all(hg.nodes.len()),
        }
    }

    /// Solves the full join group and returns its cheapest complete plan.
    pub(super) fn solve(&mut self) -> OptimizeResult<Option<PlanState<M::Cost>>> {
        self.solve_subset(&NodeSet::all(self.hg.nodes.len()))
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
            let root = self.hg.nodes[i].root;
            let cost = self.cost_model.total_cost(root, self.ctx, self.analyses)?;
            self.dp.insert(
                s,
                PlanState {
                    root,
                    cost,
                    #[cfg(test)]
                    tree: JoinTree::Leaf(i),
                },
            );
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

        let edge_indices = connecting_edge_indices(s1, s2, self.hg);
        if edge_indices.is_empty() {
            return Ok(());
        }

        let Some(new_state) = best_join_candidate(
            self.ctx,
            self.analyses,
            self.hg,
            self.cost_model,
            s1,
            &left,
            s2,
            &right,
            &edge_indices,
        )?
        else {
            return Ok(());
        };
        let combined = s1 | s2;

        let better = self
            .dp
            .get(&combined)
            .is_none_or(|existing| self.cost_model.is_better(&new_state.cost, &existing.cost));

        if better {
            self.dp.insert(combined, new_state);
        }
        Ok(())
    }

    /// Returns DPhyp's neighborhood `N(s, x)` inside the current induced subgraph.
    fn neighborhood(&self, s: &NodeSet, x: &NodeSet) -> NodeSet {
        JoinGraph::new(self.hg).neighborhood_within(s, x, &self.allowed)
    }

    /// Returns whether an applicable hyperedge connects `s1` and `s2`.
    fn has_edge(&self, s1: &NodeSet, s2: &NodeSet) -> bool {
        JoinGraph::new(self.hg).connects(s1, s2)
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
