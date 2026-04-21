use super::{extract_bitset, subset::subsets};
use bitvec::prelude::*;
use std::{collections::HashMap, sync::Arc};

mod hypergraph;

pub use hypergraph::*;

#[derive(Debug, Clone)]
pub enum JoinTree {
    Scan(usize),
    Join(Arc<JoinTree>, Arc<JoinTree>, EdgeSet),
}

pub struct DPHyp {
    dp_table: HashMap<VertexSet, Arc<JoinTree>>,
    pairs: Vec<(VertexSet, VertexSet, EdgeSet)>,
}

impl Default for DPHyp {
    fn default() -> Self {
        Self::new()
    }
}

impl DPHyp {
    pub fn new() -> Self {
        DPHyp {
            dp_table: HashMap::new(),
            pairs: Vec::new(),
        }
    }

    pub fn get_pairs(&self) -> &Vec<(VertexSet, VertexSet, EdgeSet)> {
        &self.pairs
    }

    pub fn solve<V, E>(&mut self, h: &QueryHypergraph<V, E>) -> Option<Arc<JoinTree>> {
        // Initialize the dp table with plans for single relations.
        for v in 0..h.vertices.len() {
            let mut bitvec = BitVec::repeat(false, h.vertices.len());
            bitvec.set(v, true);

            let touching_edges = &h.vertices[v].edges.clone();
            let mut satisfying_edges = h.empty_edge_set();

            // For leaves, check if any edge is fully satisfied by the single vertex
            for edge_idx in touching_edges.iter_ones() {
                let edge = &h.edges[edge_idx];
                // For simple edges, this is true. For hyperedges, it might be false.
                // We check if the requirement 'u' or 'v' is exactly this vertex {v}.
                if edge.u[v] && edge.u.count_ones() == 1 || edge.v[v] && edge.v.count_ones() == 1 {
                    satisfying_edges.set(edge_idx, true);
                }
            }

            self.dp_table.insert(bitvec, Arc::new(JoinTree::Scan(v)));
        }

        for v in (0..h.vertices.len()).rev() {
            // Generate all csg-cmp-pairs ({v}, S2) by calling
            let mut singelton = BitVec::repeat(false, h.vertices.len());
            singelton.set(v, true);
            self.emit_csg(singelton.clone(), h);

            // Extend the initial set {v} to larger sets S1,
            // then find connected subsets of its complement S2 such that
            // each distinct (S1, S2) results in a csg-cmp-pair.
            // Similar to DPccp, prohibit B_v = {w | w < v} U {v}
            let b_v = h.b_i_set(v);
            self.enumerate_csg_rec(singelton, b_v, h);
        }

        let root = bitvec![1; h.vertices.len()];
        self.dp_table.get(&root).cloned()
    }

    fn enumerate_csg_rec<V, E>(&mut self, s1: VertexSet, x: VertexSet, h: &QueryHypergraph<V, E>) {
        let neighborhood = h.neighborhood(s1.clone(), x.clone());
        for n in subsets(&neighborhood) {
            let s1_new = n | &s1;
            if self.dp_table.contains_key(&s1_new) {
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
            if self.dp_table.contains_key(&s2_new) && h.is_connected(&s1, &s2_new) {
                self.emit_csg_cmp(s1.clone(), s2_new.clone(), h);
            }
        }
        let x_new = x | &neighborhood;
        for n in subsets(&neighborhood) {
            let s2_new = n | &s2;
            self.enumerate_cmp_rec(s1.clone(), s2_new, x_new.clone(), h);
        }
    }

    /// This is currently mocked. Need to get the predicates and compute cost.
    fn emit_csg_cmp<V, E>(&mut self, s1: VertexSet, s2: VertexSet, h: &QueryHypergraph<V, E>) {
        let plan_1 = self
            .dp_table
            .get(&s1)
            .expect("s1 should have a plan")
            .clone();

        let plan_2 = self
            .dp_table
            .get(&s2)
            .expect("s2 should have a plan")
            .clone();

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
        let induced_edges = h.get_induced_edges(s.clone());

        self.pairs.push((s1, s2, induced_edges));

        // TODO: compute cost.
        let new_plan = JoinTree::Join(plan_1, plan_2, join_edges);
        self.dp_table.insert(s, Arc::new(new_plan));
    }
}

pub fn make_example_hypergraph() -> QueryHypergraph<&'static str, &'static str> {
    let mut h = QueryHypergraph::new();
    h.add_vertex("R1");
    h.add_vertex("R2");
    h.add_vertex("R3");
    h.add_vertex("R4");
    h.add_vertex("R5");
    h.add_vertex("R6");

    let e = h.add_edge(
        "(R1.#1=R2.#1)",
        bitvec![1, 0, 0, 0, 0, 0],
        bitvec![0, 1, 0, 0, 0, 0],
    );
    assert!(h.edges[e].is_simple());
    let e = h.add_edge(
        "(R2.#2=R3.#2)",
        bitvec![0, 1, 0, 0, 0, 0],
        bitvec![0, 0, 1, 0, 0, 0],
    );
    assert!(h.edges[e].is_simple());
    let e = h.add_edge(
        "(R4.#3=R5.#3)",
        bitvec![0, 0, 0, 1, 0, 0],
        bitvec![0, 0, 0, 0, 1, 0],
    );
    assert!(h.edges[e].is_simple());
    let e = h.add_edge(
        "(R5.#4=R6.#4)",
        bitvec![0, 0, 0, 0, 1, 0],
        bitvec![0, 0, 0, 0, 0, 1],
    );
    assert!(h.edges[e].is_simple());
    let e = h.add_edge(
        "(R1.a + R2.b + R3.c = R4.d + R5.e + R6.f)",
        bitvec![1, 1, 1, 0, 0, 0],
        bitvec![0, 0, 0, 1, 1, 1],
    );
    assert!(!h.edges[e].is_simple());
    h
}

pub fn show_csg_cmp_pairs<V, E>(
    query_graph: &QueryHypergraph<V, E>,
    pairs: Vec<(VertexSet, VertexSet, EdgeSet)>,
    mut f: impl std::io::Write,
) where
    V: std::fmt::Debug,
    E: std::fmt::Debug,
{
    f.write_fmt(format_args!("All csg-cmp pairs:\n"))
        .expect("write failed");
    let n = pairs.len();
    for (csg, cmp, edge_mask) in pairs {
        let csg = extract_bitset(&csg)
            .iter()
            .map(|v| query_graph.get_vertex_info(*v).unwrap())
            .collect::<Vec<_>>();

        let cmp = extract_bitset(&cmp)
            .iter()
            .map(|v| query_graph.get_vertex_info(*v).unwrap())
            .collect::<Vec<_>>();
        f.write_fmt(format_args!("{:?},{:?}\n", csg, cmp))
            .expect("write failed");

        let edges = query_graph.get_edges(edge_mask);
        f.write_fmt(format_args!("  edges: {:?}\n", edges))
            .expect("write failed");
    }

    f.write_fmt(format_args!("Total {} pairs.\n", n))
        .expect("write failed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dp_hyp() {
        let h = make_example_hypergraph();
        let mut algo = DPHyp::new();
        let plan = algo.solve(&h);
        assert!(plan.is_some(), "DPHyp should find a plan");

        let file = std::io::stdout();
        show_csg_cmp_pairs(&h, algo.pairs.clone(), file);
    }
}
