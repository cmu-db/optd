//! Hypergraph representation used by DPHyp.
//!
//! Each hyperedge is stored as an ordered cut `(u, v)`. For simple binary
//! predicates both sides are singletons; for n-ary predicates each side may
//! contain multiple leaves.

use bitvec::prelude::*;

use super::super::{BitVecSetOpsExt, EdgeIndex, EdgeSet, VertexIndex, VertexSet};

/// Query hypergraph whose edge payload is typically a predicate identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryHypergraph<V, E> {
    pub(crate) vertices: Vec<Vertex<V>>,
    pub(crate) edges: Vec<Edge<E>>,
}

impl<V, E> Default for QueryHypergraph<V, E> {
    fn default() -> Self {
        Self::new()
    }
}

/// Vertex payload plus its incident-edge bitset.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Vertex<V> {
    pub info: V,
    /// Indices of the edges connected to this vertex.
    pub(crate) edges: BitVec,
}

/// Hyperedge payload plus the two endpoint subsets it relates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Edge<E> {
    pub info: E,
    /// Indices of the vertices this edge connects.
    pub(crate) u: BitVec,
    pub(crate) v: BitVec,
}

impl<E> Edge<E> {
    /// Checks whether this edge can be used to connect `s1` and `s2`.
    ///
    /// This is stricter than “shares a vertex”: one side of the edge must be
    /// fully contained in one set and the other side fully contained in the
    /// other set.
    pub fn connects(&self, s1: VertexSet, s2: VertexSet) -> bool {
        if !(self.u.any() && self.v.any()) {
            return false;
        }
        if self.u.is_subset_of(&s1) && self.v.is_subset_of(&s2) {
            return true;
        }
        if self.v.is_subset_of(&s1) && self.u.is_subset_of(&s2) {
            return true;
        }
        false
    }

    /// Returns one representative neighbor reachable from `s` without entering
    /// the forbidden set `x`.
    ///
    /// For multi-vertex hypernodes, DPHyp expands the representative first and
    /// relies on recursive enumeration to recover the remaining vertices.
    pub fn get_neighbor(&self, s: VertexSet, x: VertexSet) -> Option<usize> {
        if self.u.is_subset_of(&s) && !self.v.intersects(&s) {
            let other = &self.v;
            if !other.intersects(&x) {
                // Note: If other is empty, first_one() will return None, which is the desired behavior.
                return other.first_one();
            }
        } else if self.v.is_subset_of(&s) && !self.u.intersects(&s) {
            let other = &self.u;
            if !other.intersects(&x) {
                // Note: if other is empty, first_one() will return None, which is the desired behavior.
                return other.first_one();
            }
        }
        None
    }

    /// Check if the hyperedge is simple, i.e., |u| = |v| = 1.
    pub fn is_simple(&self) -> bool {
        self.u.count_ones() == 1 && self.v.count_ones() == 1
    }

    /// Returns the full set of vertices mentioned by the edge.
    pub fn vertex_set(&self) -> VertexSet {
        self.u.clone() | &self.v
    }
}

impl<V, E> QueryHypergraph<V, E> {
    /// Creates an empty hypergraph.
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Returns the payload stored for a vertex.
    pub fn get_vertex_info(&self, vertex_index: VertexIndex) -> Option<&V> {
        self.vertices.get(vertex_index).map(|v| &v.info)
    }

    /// Returns the payload stored for an edge.
    pub fn get_edge_info(&self, edge_index: EdgeIndex) -> Option<&E> {
        self.edges.get(edge_index).map(|e| &e.info)
    }

    /// Allocates an empty vertex bitset of the correct length.
    pub fn empty_vertex_set(&self) -> VertexSet {
        bitvec![0; self.vertices.len()]
    }

    /// Allocates an empty edge bitset of the correct length.
    pub fn empty_edge_set(&self) -> EdgeSet {
        bitvec![0; self.edges.len()]
    }

    /// Returns `B_i = {0, ..., i}` used by DPHyp's duplicate-avoidance rules.
    pub fn b_i_set(&self, i: usize) -> VertexSet {
        let mut b_i = bitvec![0; self.vertices.len()];
        b_i[0..=i].fill(true);
        b_i
    }

    /// Adds a vertex and returns its index.
    pub fn add_vertex(&mut self, info: V) -> VertexIndex {
        let vertex_index = self.vertices.len();
        self.vertices.push(Vertex {
            info,
            edges: BitVec::new(),
        });
        vertex_index
    }

    /// Adds a hyperedge connecting `u` to `v`.
    ///
    /// Callers are responsible for supplying the intended two-sided
    /// decomposition of the predicate; the hypergraph preserves it verbatim.
    pub fn add_edge(&mut self, info: E, u: VertexSet, v: VertexSet) -> EdgeIndex {
        assert!(
            u.count_ones() >= 1 && v.count_ones() >= 1,
            "An edge must connect at least two vertices."
        );

        let mut edge_vertices = bitvec![0; self.vertices.len()];
        let edge_index = self.edges.len();

        for vertex_index in u.iter_ones().chain(v.iter_ones()) {
            assert!(
                vertex_index < self.vertices.len(),
                "Vertex index {} out of bounds.",
                vertex_index
            );
            edge_vertices.set(vertex_index, true);
            let vertex = &mut self.vertices[vertex_index];

            vertex.edges.resize(self.edges.len() + 1, false);
            vertex.edges.set(edge_index, true);
        }

        self.edges.push(Edge { info, u, v });

        edge_index
    }

    /// Returns whether two vertex sets are disjoint.
    pub fn is_disjoint(&self, a: VertexSet, b: VertexSet) -> bool {
        (a & b).not_any()
    }

    /// Returns whether at least one hyperedge can connect the two sets.
    pub fn is_connected(&self, s1: &VertexSet, s2: &VertexSet) -> bool {
        for v in s1.iter_ones() {
            let vertex = &self.vertices[v];
            for edge_index in vertex.edges.iter_ones() {
                let edge = &self.edges[edge_index];
                if edge.connects(s1.clone(), s2.clone()) {
                    return true;
                }
            }
        }
        false
    }

    /// Returns representative neighbors reachable from `s` while excluding `x`.
    pub fn neighborhood(&self, s: VertexSet, x: VertexSet) -> VertexSet {
        let mut result = self.empty_vertex_set();
        for v in s.iter_ones() {
            let vertex = &self.vertices[v];
            for edge_index in vertex.edges.iter_ones() {
                let edge = &self.edges[edge_index];
                if let Some(neighbor) = edge.get_neighbor(s.clone(), x.clone()) {
                    result.set(neighbor, true);
                }
            }
        }
        result
    }

    /// Resolves an edge mask to edge payloads.
    pub fn get_edges(&self, edge_mask: EdgeSet) -> Vec<&E> {
        edge_mask
            .iter_ones()
            .map(|edge_index| {
                &self
                    .edges
                    .get(edge_index)
                    .expect("Edge index out of bounds")
                    .info
            })
            .collect()
    }

    /// Returns the edges fully contained in `s`.
    pub fn get_induced_edges(&self, s: VertexSet) -> EdgeSet {
        let mut edge_mask = self.empty_edge_set();
        for (i, edge) in self.edges.iter().enumerate() {
            if edge.vertex_set().is_subset_of(&s) {
                edge_mask.set(i, true);
            }
        }
        edge_mask
    }
}

#[cfg(test)]
mod tests {

    use crate::rules::join_ordering::fixtures::make_dphyp_example_hypergraph;

    use super::*;

    #[test]
    fn test_query_hypergraph_connectedness() {
        let h = make_dphyp_example_hypergraph();
        assert!(
            !h.is_connected(&bitvec![1, 1, 1, 0, 0, 0], &bitvec![0, 0, 0, 1, 0, 0]),
            "R1, R2, R3 should not be connected to R4 alone"
        );

        assert!(
            !h.is_connected(&bitvec![1, 1, 1, 0, 0, 0], &bitvec![0, 0, 0, 1, 1, 0]),
            "R1, R2, R3 should not be connected to R4 and R5 alone"
        );

        assert!(
            h.is_connected(&bitvec![1, 1, 1, 0, 0, 0], &bitvec![0, 0, 0, 1, 1, 1]),
            "R1, R2, R3 should  be connected to R4, R5, R6 through hyperedge"
        );
    }

    #[test]
    fn test_query_hypergraph_neighborhood() {
        let h = make_dphyp_example_hypergraph();

        let n = h.neighborhood(bitvec![1, 1, 1, 0, 0, 0], bitvec![1, 1, 1, 0, 0, 0]);
        assert_eq!(
            n,
            bitvec![0, 0, 0, 1, 0, 0],
            "The neighborhood of R1, R2, R3 should be R4 (index 3), the representative of the hypernode [R4, R5, R6]"
        );

        let n = h.neighborhood(bitvec![0, 1, 0, 0, 0, 0], bitvec![1, 0, 0, 0, 0, 0]);
        assert_eq!(
            n,
            bitvec![0, 0, 1, 0, 0, 0],
            "The neighborhood of R2 should be R3 (index 2)"
        );

        let n = h.neighborhood(bitvec![0, 1, 1, 0, 0, 0], bitvec![1, 0, 0, 0, 0, 0]);
        assert_eq!(n, bitvec![0; 6],);

        let n = h.neighborhood(bitvec![0, 0, 0, 1, 0, 0], bitvec![0;6]);
        assert_eq!(n, bitvec![0, 0, 0, 0, 1, 0],);

        let n = h.neighborhood(bitvec![0, 0, 0, 1, 1, 0], bitvec![0;6]);
        assert_eq!(n, bitvec![0, 0, 0, 0, 0, 1],);
    }
}
