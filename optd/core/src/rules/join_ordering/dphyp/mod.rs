//! DPHyp pair enumeration over query hypergraphs.
//!
//! This module deliberately stops at enumeration. It records which connected
//! subsets exist and emits the `(left, right, join_edges)` combinations that a
//! higher layer can cost.

use super::{EdgeSet, VertexSet, debug_vertex_set, subsets, write_csg_cmp_pairs};
use bitvec::prelude::*;
use std::collections::HashSet;

mod hypergraph;

pub use hypergraph::*;

/// One emitted DPHyp combine opportunity.
///
/// `join_edges` contains only the edges that directly connect `left` and
/// `right`, not all edges induced by `left ∪ right`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumeratedPair {
    pub left: VertexSet,
    pub right: VertexSet,
    pub join_edges: EdgeSet,
}

/// DPHyp enumerator state.
///
/// `seen_subgraphs` tracks which connected subsets have been discovered so far;
/// it is not a costed DP memo.
pub struct DPHyp {
    seen_subgraphs: HashSet<VertexSet>,
    pairs: Vec<EnumeratedPair>,
}

impl Default for DPHyp {
    fn default() -> Self {
        Self::new()
    }
}

impl DPHyp {
    /// Creates a fresh enumerator.
    pub fn new() -> Self {
        DPHyp {
            seen_subgraphs: HashSet::new(),
            pairs: Vec::new(),
        }
    }

    /// Returns the emitted pairs in enumeration order.
    pub fn get_pairs(&self) -> &[EnumeratedPair] {
        &self.pairs
    }

    /// Runs DPHyp on `h` and returns whether the full vertex set became
    /// connected under the discovered subgraphs.
    pub fn solve<V, E>(&mut self, h: &QueryHypergraph<V, E>) -> bool {
        self.seen_subgraphs.clear();
        self.pairs.clear();

        for v in 0..h.vertices.len() {
            let mut bitvec = BitVec::repeat(false, h.vertices.len());
            bitvec.set(v, true);
            self.seen_subgraphs.insert(bitvec);
        }

        for v in (0..h.vertices.len()).rev() {
            let mut singleton = BitVec::repeat(false, h.vertices.len());
            singleton.set(v, true);
            self.emit_csg(singleton.clone(), h);

            let b_v = h.b_i_set(v);
            self.enumerate_csg_rec(singleton, b_v, h);
        }

        let root = bitvec![1; h.vertices.len()];
        self.seen_subgraphs.contains(&root)
    }

    /// Recursively expands a connected subgraph candidate.
    fn enumerate_csg_rec<V, E>(&mut self, s1: VertexSet, x: VertexSet, h: &QueryHypergraph<V, E>) {
        let neighborhood = h.neighborhood(s1.clone(), x.clone());
        for n in subsets(&neighborhood) {
            let s1_new = n | &s1;
            if self.seen_subgraphs.contains(&s1_new) {
                self.emit_csg(s1_new, h);
            }
        }

        let x_new = neighborhood.clone() | &x;
        for n in subsets(&neighborhood) {
            let s1_new = n | &s1;
            // expand each subset of the neighborhood further.
            self.enumerate_csg_rec(s1_new, x_new.clone(), h);
        }
    }

    /// Emits all complements reachable from one connected subgraph `s1`.
    fn emit_csg<V, E>(&mut self, s1: VertexSet, h: &QueryHypergraph<V, E>) {
        let min_s1 = s1.first_one().expect("s1 should not be empty");
        let x = h.b_i_set(min_s1) | &s1;
        let n = h.neighborhood(s1.clone(), x.clone());

        let mut new_x = x | &n;

        for v in n.iter_ones().rev() {
            let mut s2 = h.empty_vertex_set();
            s2.set(v, true);

            if h.is_connected(&s1, &s2) {
                self.emit_csg_cmp(s1.clone(), s2.clone(), h);
            }

            self.enumerate_cmp_rec(s1.clone(), s2, new_x.clone(), h);
            new_x.set(v, false);
        }
    }

    /// Recursively expands complement candidates for a fixed left side `s1`.
    fn enumerate_cmp_rec<V, E>(
        &mut self,
        s1: VertexSet,
        s2: VertexSet,
        x: VertexSet,
        h: &QueryHypergraph<V, E>,
    ) {
        let neighborhood = h.neighborhood(s2.clone(), x.clone());
        for n in subsets(&neighborhood) {
            let s2_new = n | &s2;
            if self.seen_subgraphs.contains(&s2_new) && h.is_connected(&s1, &s2_new) {
                self.emit_csg_cmp(s1.clone(), s2_new.clone(), h);
            }
        }
        let x_new = x | &neighborhood;
        for n in subsets(&neighborhood) {
            let s2_new = n | &s2;
            self.enumerate_cmp_rec(s1.clone(), s2_new, x_new.clone(), h);
        }
    }

    /// Records one legal combine and marks the union as discovered.
    fn emit_csg_cmp<V, E>(&mut self, s1: VertexSet, s2: VertexSet, h: &QueryHypergraph<V, E>) {
        let mut join_edges = h.empty_edge_set();
        for (edge_index, edge) in h.edges.iter().enumerate() {
            if edge.connects(s1.clone(), s2.clone()) {
                join_edges.set(edge_index, true);
            }
        }

        assert!(
            join_edges.count_ones() > 0,
            "There should be at least one edge connecting s1 and s2"
        );

        let s = s1.clone() | &s2;
        self.pairs.push(EnumeratedPair {
            left: s1,
            right: s2,
            join_edges,
        });
        self.seen_subgraphs.insert(s);
    }
}

/// Pretty-printer for the emitted DPHyp pairs.
pub fn show_csg_cmp_pairs<V, E>(
    query_graph: &QueryHypergraph<V, E>,
    pairs: Vec<EnumeratedPair>,
    mut f: impl std::io::Write,
) where
    V: std::fmt::Debug,
    E: std::fmt::Debug,
{
    write_csg_cmp_pairs(&mut f, pairs, |f, pair| {
        let csg = debug_vertex_set(&pair.left, |v| query_graph.get_vertex_info(v));
        let cmp = debug_vertex_set(&pair.right, |v| query_graph.get_vertex_info(v));
        f.write_fmt(format_args!("{:?},{:?}\n", csg, cmp))?;

        let edges = query_graph.get_edges(pair.join_edges.clone());
        f.write_fmt(format_args!("  edges: {:?}\n", edges))
    })
    .expect("write failed");
}

#[cfg(test)]
mod tests {
    use crate::rules::join_ordering::fixtures::make_dphyp_example_hypergraph;

    use super::*;

    #[test]
    fn test_dp_hyp() {
        let h = make_dphyp_example_hypergraph();
        let mut algo = DPHyp::new();
        assert!(algo.solve(&h), "DPHyp should find a plan");

        let file = std::io::stdout();
        show_csg_cmp_pairs(&h, algo.pairs.clone(), file);
    }
}
