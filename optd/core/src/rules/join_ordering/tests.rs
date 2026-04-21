use std::collections::BTreeSet;

use super::VertexSet;
use super::dpccp::enumerate_csg_cmp;
use super::dphyp::DPHyp;
use super::fixtures::{
    make_example_simple_graph, make_example_simple_hypergraph, make_job_q13_graph,
    make_job_q13_hypergraph,
};

fn normalize_pair(left: &VertexSet, right: &VertexSet) -> (Vec<usize>, Vec<usize>) {
    (left.iter_ones().collect(), right.iter_ones().collect())
}

#[test]
fn test_dpccp_and_dphyp_emit_same_pairs_on_simple_graph() {
    let graph = make_example_simple_graph();
    let hypergraph = make_example_simple_hypergraph();

    let dpccp_pairs = enumerate_csg_cmp(&graph)
        .into_iter()
        .map(|(left, right)| normalize_pair(&left, &right))
        .collect::<BTreeSet<_>>();

    let mut dphyp = DPHyp::new();
    let plan = dphyp.solve(&hypergraph);
    assert!(plan.is_some(), "DPHyp should find a plan on a simple graph");

    let dphyp_pairs = dphyp
        .get_pairs()
        .iter()
        .map(|(left, right, _)| normalize_pair(left, right))
        .collect::<BTreeSet<_>>();

    assert_eq!(dpccp_pairs, dphyp_pairs);
}

#[test]
fn test_dpccp_and_dphyp_emit_same_pairs_on_job_q13() {
    let graph = make_job_q13_graph();
    let hypergraph = make_job_q13_hypergraph();

    let dpccp_pairs = enumerate_csg_cmp(&graph)
        .into_iter()
        .map(|(left, right)| normalize_pair(&left, &right))
        .collect::<BTreeSet<_>>();

    let mut dphyp = DPHyp::new();
    let plan = dphyp.solve(&hypergraph);
    assert!(plan.is_some(), "DPHyp should find a plan on JOB Query 13d");

    let dphyp_pairs = dphyp
        .get_pairs()
        .iter()
        .map(|(left, right, _)| normalize_pair(left, right))
        .collect::<BTreeSet<_>>();

    assert_eq!(dpccp_pairs, dphyp_pairs);
}
