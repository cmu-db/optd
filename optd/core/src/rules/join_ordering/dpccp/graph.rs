use bitvec::prelude::*;

use super::super::{EdgeIndex, VertexIndex, VertexSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryGraph<V, E> {
    pub(crate) vertices: Vec<Vertex<V>>,
    pub(crate) edges: Vec<Edge<E>>,
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
    pub(crate) vertices: BitVec,
}

impl<E> Edge<E> {
    /// A simple function to check if the edge connects two vertices.
    pub fn connects(&self, a: VertexIndex, b: VertexIndex) -> bool {
        self.vertices[a] && self.vertices[b]
    }
}

impl<V, E> Default for QueryGraph<V, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V, E> QueryGraph<V, E> {
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

    pub fn add_edge(&mut self, info: E, vertex_set: VertexSet) -> EdgeIndex {
        assert!(
            vertex_set.count_ones() >= 2,
            "An edge must connect at least two vertices."
        );

        let mut edge_vertices = bitvec![0; self.vertices.len()];
        let edge_index = self.edges.len();

        for vertex_index in vertex_set.iter_ones() {
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

        self.edges.push(Edge {
            info,
            vertices: edge_vertices,
        });

        edge_index
    }

    pub fn neighborhood(&self, vertex_set: VertexSet) -> VertexSet {
        let mut result = bitvec![0; self.vertices.len()];
        for vertex_index in vertex_set.iter_ones() {
            let vertex = &self.vertices[vertex_index];
            for edge_index in vertex.edges.iter_ones() {
                let edge = &self.edges[edge_index];
                result |= &edge.vertices;
            }
        }

        result &= !vertex_set;
        result
    }

    pub fn get_any_edge_between(&self, a: VertexSet, b: VertexSet) -> Option<EdgeIndex> {
        for edge_index in a
            .iter_ones()
            .flat_map(|i| self.vertices[i].edges.iter_ones())
        {
            let edge = &self.edges[edge_index];
            if (edge.vertices.clone() & &b).any() {
                return Some(edge_index);
            }
        }
        None
    }

    pub fn is_disjoint(&self, a: VertexSet, b: VertexSet) -> bool {
        (a & b).not_any()
    }

    pub fn is_connected(&self, vertex_set: &VertexSet) -> bool {
        if vertex_set.count_ones() <= 1 {
            return true;
        }

        let mut visited = bitvec![0; self.vertices.len()];
        let start = vertex_set
            .iter_ones()
            .next()
            .expect("vertex_set is not empty");
        let mut stack = vec![start];
        visited.set(start, true);

        while let Some(current) = stack.pop() {
            let vertex = &self.vertices[current];
            for edge_index in vertex.edges.iter_ones() {
                let edge = &self.edges[edge_index];
                for neighbor_index in edge.vertices.iter_ones() {
                    if vertex_set[neighbor_index] && !visited[neighbor_index] {
                        visited.set(neighbor_index, true);
                        stack.push(neighbor_index);
                    }
                }
            }
        }

        for vertex_index in vertex_set.iter_ones() {
            if !visited[vertex_index] {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_graph() {
        let mut graph = QueryGraph::new();
        let v0 = graph.add_vertex("A");
        let v1 = graph.add_vertex("B");
        let v2 = graph.add_vertex("C");
        let e0 = graph.add_edge("R1", bitvec![1, 1, 0]);
        let e1 = graph.add_edge("R2", bitvec![0, 1, 1]);

        assert_eq!(graph.vertices[v0].info, "A");
        assert_eq!(graph.vertices[v1].info, "B");
        assert_eq!(graph.edges[e0].info, "R1");
        assert!(graph.edges[e0].connects(v0, v1));
        assert!(graph.edges[e1].connects(v1, v2));
        let neighborhood = graph.neighborhood(bitvec![0, 1, 0]);
        assert_eq!(neighborhood, bitvec![1, 0, 1]);

        let neighborhood = graph.neighborhood(bitvec![1, 0, 0]);
        assert_eq!(neighborhood, bitvec![0, 1, 0]);
    }
}
