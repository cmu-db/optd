mod graph;

pub use graph::*;

use bitvec::prelude::*;
use tracing::{info, info_span, instrument};

use super::{extract_bitset, subset::subsets};

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

pub fn show_csg_cmp_pairs<V, E>(
    query_graph: &QueryGraph<V, E>,
    pairs: Vec<(BitVec, BitVec)>,
    mut f: impl std::io::Write,
) where
    V: std::fmt::Debug,
    E: std::fmt::Debug,
{
    f.write_fmt(format_args!("All csg-cmp pairs:\n"))
        .expect("write failed");
    let n = pairs.len();
    for (csg, cmp) in pairs {
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
    }

    f.write_fmt(format_args!("Total {} pairs.\n", n))
        .expect("write failed");
}
#[cfg(test)]
mod tests {

    use tracing_test::traced_test;

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
        let mut graph = QueryGraph::new();
        graph.add_vertex("R0");
        graph.add_vertex("R1");
        graph.add_vertex("R2");
        graph.add_vertex("R3");
        graph.add_vertex("R4");

        // 0, 1
        graph.add_edge("R0-R1", bitvec![1, 1, 0, 0, 0]);
        // 0, 2
        graph.add_edge("R0-R2", bitvec![1, 0, 1, 0, 0]);
        // 0, 3
        graph.add_edge("R0-R3", bitvec![1, 0, 0, 1, 0]);
        // 1, 4
        graph.add_edge("R1-R4", bitvec![0, 1, 0, 0, 1]);
        // 2. 3
        graph.add_edge("R2-R3", bitvec![0, 0, 1, 1, 0]);
        // 2, 4
        graph.add_edge("R2-R4", bitvec![0, 0, 1, 0, 1]);
        // 3, 4
        graph.add_edge("R3-R4", bitvec![0, 0, 0, 1, 1]);

        let pairs = enumerate_csg_cmp(&graph);
        verify_no_duplicate_pairs(&pairs);
        verify_csg_cmp_definition(&graph, &pairs);
        show_csg_cmp_pairs(&graph, pairs, std::io::stdout());
    }
}
