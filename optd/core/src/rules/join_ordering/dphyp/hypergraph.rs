use bitvec::prelude::*;

use super::super::{BitVecSetOpsExt, EdgeIndex, EdgeSet, VertexIndex, VertexSet};

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Vertex<V> {
    pub info: V,
    /// Indices of the edges connected to this vertex.
    pub(crate) edges: BitVec,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Edge<E> {
    pub info: E,
    /// Indices of the vertices this edge connects.
    pub(crate) u: BitVec,
    pub(crate) v: BitVec,
}

impl<E> Edge<E> {
    /// Checks if the edge connects two vertex sets.
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

    /// If the edge connects s to some vertex outside of x, return the index of one such vertex. Otherwise, return None.
    /// The returned index is the representative, or more concretely, the minimum index of the vertices in the hypernode.
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

    /// Returns all the involving vertices in the hyperedge.
    pub fn vertex_set(&self) -> VertexSet {
        self.u.clone() | &self.v
    }
}

impl<V, E> QueryHypergraph<V, E> {
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn get_vertex_info(&self, vertex_index: VertexIndex) -> Option<&V> {
        self.vertices.get(vertex_index).map(|v| &v.info)
    }

    pub fn get_edge_info(&self, edge_index: EdgeIndex) -> Option<&E> {
        self.edges.get(edge_index).map(|e| &e.info)
    }

    pub fn empty_vertex_set(&self) -> VertexSet {
        bitvec![0; self.vertices.len()]
    }

    pub fn empty_edge_set(&self) -> EdgeSet {
        bitvec![0; self.edges.len()]
    }

    pub fn b_i_set(&self, i: usize) -> VertexSet {
        let mut b_i = bitvec![0; self.vertices.len()];
        b_i[0..=i].fill(true);
        b_i
    }

    pub fn add_vertex(&mut self, info: V) -> VertexIndex {
        let vertex_index = self.vertices.len();
        self.vertices.push(Vertex {
            info,
            edges: BitVec::new(),
        });
        vertex_index
    }

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

    pub fn is_disjoint(&self, a: VertexSet, b: VertexSet) -> bool {
        (a & b).not_any()
    }

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
