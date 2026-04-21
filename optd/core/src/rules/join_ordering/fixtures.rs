#[cfg(test)]
use bitvec::prelude::*;

#[cfg(test)]
use super::dpccp::QueryGraph;
#[cfg(test)]
use super::dphyp::QueryHypergraph;

#[cfg(test)]
pub(crate) fn make_example_simple_graph() -> QueryGraph<&'static str, &'static str> {
    let mut graph = QueryGraph::new();
    graph.add_vertex("R0");
    graph.add_vertex("R1");
    graph.add_vertex("R2");
    graph.add_vertex("R3");
    graph.add_vertex("R4");

    graph.add_edge("R0-R1", bitvec![1, 1, 0, 0, 0]);
    graph.add_edge("R0-R2", bitvec![1, 0, 1, 0, 0]);
    graph.add_edge("R0-R3", bitvec![1, 0, 0, 1, 0]);
    graph.add_edge("R1-R4", bitvec![0, 1, 0, 0, 1]);
    graph.add_edge("R2-R3", bitvec![0, 0, 1, 1, 0]);
    graph.add_edge("R2-R4", bitvec![0, 0, 1, 0, 1]);
    graph.add_edge("R3-R4", bitvec![0, 0, 0, 1, 1]);

    graph
}

#[cfg(test)]
pub(crate) fn make_example_simple_hypergraph() -> QueryHypergraph<&'static str, &'static str> {
    let mut h = QueryHypergraph::new();
    h.add_vertex("R0");
    h.add_vertex("R1");
    h.add_vertex("R2");
    h.add_vertex("R3");
    h.add_vertex("R4");

    h.add_edge("R0-R1", bitvec![1, 0, 0, 0, 0], bitvec![0, 1, 0, 0, 0]);
    h.add_edge("R0-R2", bitvec![1, 0, 0, 0, 0], bitvec![0, 0, 1, 0, 0]);
    h.add_edge("R0-R3", bitvec![1, 0, 0, 0, 0], bitvec![0, 0, 0, 1, 0]);
    h.add_edge("R1-R4", bitvec![0, 1, 0, 0, 0], bitvec![0, 0, 0, 0, 1]);
    h.add_edge("R2-R3", bitvec![0, 0, 1, 0, 0], bitvec![0, 0, 0, 1, 0]);
    h.add_edge("R2-R4", bitvec![0, 0, 1, 0, 0], bitvec![0, 0, 0, 0, 1]);
    h.add_edge("R3-R4", bitvec![0, 0, 0, 1, 0], bitvec![0, 0, 0, 0, 1]);

    h
}

#[cfg(test)]
pub(crate) fn make_dphyp_example_hypergraph() -> QueryHypergraph<&'static str, &'static str> {
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

// -- JOB Query 13d
// SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie
// FROM company_name AS cn,
// company_type AS ct,
// info_type AS it,
// info_type AS it2,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS miidx,
// title AS t
// WHERE cn.country_code = '[us]'
// AND ct.kind = 'production companies'
// AND it.info = 'rating'
// AND it2.info = 'release dates'
// AND kt.kind = 'movie'
// AND mi.movie_id = t.id
// AND it2.id = mi.info_type_id
// AND kt.id = t.kind_id
// AND mc.movie_id = t.id
// AND cn.id = mc.company_id
// AND ct.id = mc.company_type_id
// AND miidx.movie_id = t.id
// AND it.id = miidx.info_type_id
// AND mi.movie_id = miidx.movie_id
// AND mi.movie_id = mc.movie_id
// AND miidx.movie_id = mc.movie_id;
#[cfg(test)]
pub(crate) fn make_job_q13_hypergraph() -> QueryHypergraph<&'static str, &'static str> {
    let mut graph = QueryHypergraph::new();
    ["cn", "ct", "it", "it2", "t", "kt", "mc", "mi", "miidx"]
        .iter()
        .for_each(|v| {
            graph.add_vertex(*v);
        });

    // mi.movie_id = t.id
    graph.add_edge(
        "mi.movie_id = t.id",
        bitvec![0, 0, 0, 0, 0, 0, 0, 1, 0],
        bitvec![0, 0, 0, 0, 1, 0, 0, 0, 0],
    );
    // it2.id = mi.info_type_id
    graph.add_edge(
        "it2.id = mi.info_type_id",
        bitvec![0, 0, 0, 1, 0, 0, 0, 0, 0],
        bitvec![0, 0, 0, 0, 0, 0, 0, 1, 0],
    );

    // kt.id = t.kind_id
    graph.add_edge(
        "kt.id = t.kind_id",
        bitvec![0, 0, 0, 0, 0, 1, 0, 0, 0],
        bitvec![0, 0, 0, 0, 1, 0, 0, 0, 0],
    );

    // mc.movie_id = t.id
    graph.add_edge(
        "mc.movie_id = t.id",
        bitvec![0, 0, 0, 0, 0, 0, 1, 0, 0],
        bitvec![0, 0, 0, 0, 1, 0, 0, 0, 0],
    );

    // cn.id = mc.company_id
    graph.add_edge(
        "cn.id = mc.company_id",
        bitvec![1, 0, 0, 0, 0, 0, 0, 0, 0],
        bitvec![0, 0, 0, 0, 0, 0, 1, 0, 0],
    );

    // ct.id = mc.company_type_id
    graph.add_edge(
        "ct.id = mc.company_type_id",
        bitvec![0, 1, 0, 0, 0, 0, 0, 0, 0],
        bitvec![0, 0, 0, 0, 0, 0, 1, 0, 0],
    );

    // miidx.movie_id = t.id
    graph.add_edge(
        "miidx.movie_id = t.id",
        bitvec![0, 0, 0, 0, 0, 0, 0, 0, 1],
        bitvec![0, 0, 0, 0, 1, 0, 0, 0, 0],
    );

    // it.id = miidx.info_type_id
    graph.add_edge(
        "it.id = miidx.info_type_id",
        bitvec![0, 0, 1, 0, 0, 0, 0, 0, 0],
        bitvec![0, 0, 0, 0, 0, 0, 0, 0, 1],
    );

    // mi.movie_id = miidx.movie_id
    graph.add_edge(
        "mi.movie_id = miidx.movie_id",
        bitvec![0, 0, 0, 0, 0, 0, 0, 1, 0],
        bitvec![0, 0, 0, 0, 0, 0, 0, 0, 1],
    );

    // mi.movie_id = mc.movie_id
    graph.add_edge(
        "mi.movie_id = mc.movie_id",
        bitvec![0, 0, 0, 0, 0, 0, 1, 0, 0],
        bitvec![0, 0, 0, 0, 0, 0, 0, 1, 0],
    );

    //  miidx.movie_id = mc.movie_id
    graph.add_edge(
        "miidx.movie_id = mc.movie_id",
        bitvec![0, 0, 0, 0, 0, 0, 0, 0, 1],
        bitvec![0, 0, 0, 0, 0, 0, 1, 0, 0],
    );

    graph
}

#[cfg(test)]
pub(crate) fn make_job_q13_graph() -> QueryGraph<&'static str, &'static str> {
    let mut graph = QueryGraph::new();
    ["cn", "ct", "it", "it2", "t", "kt", "mc", "mi", "miidx"]
        .iter()
        .for_each(|v| {
            graph.add_vertex(*v);
        });

    // mi.movie_id = t.id
    graph.add_edge("mi.movie_id = t.id", bitvec![0, 0, 0, 0, 1, 0, 0, 1, 0]);
    // it2.id = mi.info_type_id
    graph.add_edge(
        "it2.id = mi.info_type_id",
        bitvec![0, 0, 0, 1, 0, 0, 0, 1, 0],
    );

    // kt.id = t.kind_id
    graph.add_edge("kt.id = t.kind_id", bitvec![0, 0, 0, 0, 1, 1, 0, 0, 0]);

    // mc.movie_id = t.id
    graph.add_edge("mc.movie_id = t.id", bitvec![0, 0, 0, 0, 1, 0, 1, 0, 0]);

    // cn.id = mc.company_id
    graph.add_edge("cn.id = mc.company_id", bitvec![1, 0, 0, 0, 0, 0, 1, 0, 0]);

    // ct.id = mc.company_type_id
    graph.add_edge(
        "ct.id = mc.company_type_id",
        bitvec![0, 1, 0, 0, 0, 0, 1, 0, 0],
    );

    // miidx.movie_id = t.id
    graph.add_edge("miidx.movie_id = t.id", bitvec![0, 0, 0, 0, 1, 0, 0, 0, 1]);

    // it.id = miidx.info_type_id
    graph.add_edge(
        "it.id = miidx.info_type_id",
        bitvec![0, 0, 1, 0, 0, 0, 0, 0, 1],
    );

    // mi.movie_id = miidx.movie_id
    graph.add_edge(
        "mi.movie_id = miidx.movie_id",
        bitvec![0, 0, 0, 0, 0, 0, 0, 1, 1],
    );

    // mi.movie_id = mc.movie_id
    graph.add_edge(
        "mi.movie_id = mc.movie_id",
        bitvec![0, 0, 0, 0, 0, 0, 1, 1, 0],
    );

    //  miidx.movie_id = mc.movie_id
    graph.add_edge(
        "miidx.movie_id = mc.movie_id",
        bitvec![0, 0, 0, 0, 0, 0, 1, 0, 1],
    );

    graph
}
