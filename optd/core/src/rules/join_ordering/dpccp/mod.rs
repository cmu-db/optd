//! DPccp pair enumeration over simple query graphs.
//!
//! This module is currently used as a reference implementation and parity check
//! for `dphyp` on purely binary join predicates.

mod graph;

pub use graph::*;

use bitvec::prelude::*;
use tracing::{info, info_span, instrument};

use super::{debug_vertex_set, extract_bitset, subsets, write_csg_cmp_pairs};

/// Enumerates all valid `(CSG, CMP)` pairs for a simple query graph.
pub fn enumerate_csg_cmp<V, E>(query_graph: &QueryGraph<V, E>) -> Vec<(BitVec, BitVec)> {
    let n = query_graph.vertices.len();
    let s = bitvec![1; n];
    let x = bitvec![0; n];

    let mut pairs = Vec::new();
    enumerate_csg(query_graph, s, x, |csg| {
        enumerate_cmp(query_graph, csg, |cmp| {
            info!(
               csg = ?extract_bitset(csg),
               cmp = ?extract_bitset(cmp),
               "emit csg-cmp-pair"
            );
            pairs.push((csg.clone(), cmp.clone()));
        });
    });

    pairs
}

/// Asserts that no emitted pair appears twice, including with sides reversed.
pub fn verify_no_duplicate_pairs(pairs: &[(BitVec, BitVec)]) {
    let mut seen = std::collections::HashSet::new();
    for (csg, cmp) in pairs {
        if !seen.insert((csg.clone(), cmp.clone())) {
            panic!(
                "Duplicate pair found: csg = {:?}, cmp = {:?}",
                extract_bitset(csg),
                extract_bitset(cmp)
            );
        }
        if !seen.insert((cmp.clone(), csg.clone())) {
            panic!(
                "Duplicate pair found (reverse): csg = {:?}, cmp = {:?}",
                extract_bitset(cmp),
                extract_bitset(csg)
            );
        }
    }
}

/// Checks the defining properties of a valid `(CSG, CMP)` pair.
pub fn verify_csg_cmp_definition<V, E>(query_graph: &QueryGraph<V, E>, pairs: &[(BitVec, BitVec)]) {
    for (csg, cmp) in pairs {
        assert!(
            query_graph.is_connected(csg),
            "CSG is not connected: csg = {:?}",
            extract_bitset(csg)
        );
        assert!(
            query_graph.is_connected(cmp),
            "CMP is not connected: cmp = {:?}",
            extract_bitset(cmp)
        );
        assert!(
            query_graph.is_disjoint(csg.clone(), cmp.clone()),
            "CSG and CMP are not disjoint: csg = {:?}, cmp = {:?}",
            extract_bitset(csg),
            extract_bitset(cmp)
        );

        assert!(
            query_graph
                .get_any_edge_between(csg.clone(), cmp.clone())
                .is_some(),
            "CSG and CMP are not connected: csg = {:?}, cmp = {:?}",
            extract_bitset(csg),
            extract_bitset(cmp)
        );
    }
}

/// Enumerates all connected subgraphs reachable under DPccp's ordering rules.
pub fn enumerate_csg<V, E>(
    query_graph: &QueryGraph<V, E>,
    s: BitVec,
    x: BitVec,
    mut on_emit: impl FnMut(&BitVec),
) {
    for i in s.iter_ones().rev() {
        {
            let mut singleton = query_graph.empty_vertex_set();
            singleton.set(i, true);
            let b_i = query_graph.b_i_set(i);

            enumerate_csg_rec(
                query_graph,
                singleton,
                x.clone() | (b_i & s.clone()),
                &mut on_emit,
            );
        }
    }
}

/// Recursive worker for `enumerate_csg`.
pub fn enumerate_csg_rec<V, E>(
    query_graph: &QueryGraph<V, E>,
    s: BitVec,
    x: BitVec,
    on_emit: &mut impl FnMut(&BitVec),
) {
    let neighborhood = query_graph.neighborhood(s.clone()) & !x.clone();

    info_span!("enum_csg_rec", S = ?extract_bitset(&s), X = ?extract_bitset(&x), N = ?extract_bitset(&neighborhood))
    .in_scope(|| {
        on_emit(&s);
        let new_x = x | &neighborhood;
        for s_1 in subsets(&neighborhood) {
            let new_s = s.clone() | &s_1;
            enumerate_csg_rec(query_graph, new_s, new_x.clone(), on_emit);
        }
    })
}

#[instrument(name = "enum_cmp", skip(query_graph, on_emit), fields(csg = ?extract_bitset(csg)))]
/// Enumerates complements that can legally pair with a fixed connected subgraph.
pub fn enumerate_cmp<V, E>(
    query_graph: &QueryGraph<V, E>,
    csg: &BitVec,
    on_emit: impl FnMut(&BitVec),
) {
    let min_index = csg
        .first_one()
        .expect("query graph contains at least one relation");

    // X = B_{min(S1)} U S1
    let mut x = query_graph.b_i_set(min_index);
    x |= csg;

    // N = neighborhood(S1) \ X
    let mut n = query_graph.neighborhood(csg.clone());
    n &= !x.clone();

    enumerate_csg(query_graph, n, x, on_emit);
}

/// Pretty-printer for `(CSG, CMP)` pairs using vertex payloads.
pub fn show_csg_cmp_pairs<V, E>(
    query_graph: &QueryGraph<V, E>,
    pairs: Vec<(BitVec, BitVec)>,
    mut f: impl std::io::Write,
) where
    V: std::fmt::Debug,
    E: std::fmt::Debug,
{
    write_csg_cmp_pairs(&mut f, pairs, |f, (csg, cmp)| {
        let csg = debug_vertex_set(&csg, |v| query_graph.get_vertex_info(v));
        let cmp = debug_vertex_set(&cmp, |v| query_graph.get_vertex_info(v));
        f.write_fmt(format_args!("{:?},{:?}\n", csg, cmp))
    })
    .expect("write failed");
}
#[cfg(test)]
mod tests {

    use tracing_test::traced_test;

    use crate::rules::join_ordering::fixtures::make_example_simple_graph;

    use super::*;

    #[test]
    fn test_enumerate_csg_cmp() {
        let mut graph = QueryGraph::new();
        graph.add_vertex("lineitem");
        graph.add_vertex("order");
        graph.add_vertex("customer");
        graph.add_edge("l_orderkey = o_orderkey", bitvec![1, 1, 0]);
        graph.add_edge("o_custkey = c_custkey", bitvec![0, 1, 1]);

        assert_eq!(graph.neighborhood(bitvec![0, 1, 0]), bitvec![1, 0, 1]);
        assert_eq!(graph.neighborhood(bitvec![1, 0, 0]), bitvec![0, 1, 0]);
        assert_eq!(graph.neighborhood(bitvec![0, 0, 1]), bitvec![0, 1, 0]);

        enumerate_csg_cmp(&graph);
    }

    #[test]
    #[traced_test]
    fn test_enumerate_csg_cmp_example() {
        let graph = make_example_simple_graph();
        let pairs = enumerate_csg_cmp(&graph);
        verify_no_duplicate_pairs(&pairs);
        verify_csg_cmp_definition(&graph, &pairs);
        show_csg_cmp_pairs(&graph, pairs, std::io::stdout());
    }
}
