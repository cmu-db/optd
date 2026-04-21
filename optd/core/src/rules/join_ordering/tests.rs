use std::collections::BTreeSet;

use super::VertexSet;
use super::dpccp::enumerate_csg_cmp;
use super::dphyp::DPHyp;
use super::{JoinIsland, JoinIslandNode, JoinOrderingPass, JoinSemantics};
use super::fixtures::{
    make_example_simple_graph, make_example_simple_hypergraph, make_job_q13_graph,
    make_job_q13_hypergraph,
};
use crate::ir::{
    IRContext,
    builder::column_ref,
    explain::quick_explain,
    operator::{Get, Join, JoinImplementation, JoinType},
    statistics::TableStatistics,
    table_ref::TableRef,
    test_utils::test_ctx_with_tables,
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
    assert!(dphyp.solve(&hypergraph), "DPHyp should find a plan on a simple graph");

    let dphyp_pairs = dphyp
        .get_pairs()
        .iter()
        .map(|pair| normalize_pair(&pair.left, &pair.right))
        .collect::<BTreeSet<_>>();

    assert_eq!(dpccp_pairs, dphyp_pairs);
}

#[test]
fn test_extract_logical_join_island_keeps_constrained_joins_inside() -> crate::error::Result<()> {
    let ctx = test_ctx_with_tables(&[("t1", 1), ("t2", 1), ("t3", 1), ("t4", 1)])?;
    let t1 = ctx.logical_get(TableRef::bare("t1"), None)?.build();
    let t2 = ctx.logical_get(TableRef::bare("t2"), None)?.build();
    let t3 = ctx.logical_get(TableRef::bare("t3"), None)?.build();
    let t4 = ctx.logical_get(TableRef::bare("t4"), None)?.build();

    let inner = t1
        .clone()
        .with_ctx(&ctx)
        .logical_join(t2, crate::ir::builder::boolean(true), JoinType::Inner)
        .build();
    let semi = t3
        .clone()
        .with_ctx(&ctx)
        .logical_join(t4, crate::ir::builder::boolean(true), JoinType::LeftSemi)
        .build();
    let root = inner
        .with_ctx(&ctx)
        .logical_join(semi, crate::ir::builder::boolean(true), JoinType::LeftOuter)
        .build();

    let island = JoinIsland::extract(root, &ctx)?.expect("expected a logical join island");
    assert_eq!(island.leaf_count(), 4);
    assert_eq!(island.join_count(), 3);

    let root_atom = island.root().atom().expect("root should be a join");
    assert_eq!(root_atom.join_type, JoinType::LeftOuter);
    assert_eq!(
        root_atom.semantics,
        JoinSemantics {
            commutative: false,
            associative: false,
            preserves_left_rows: true,
            preserves_right_rows: false,
            outputs_left_only: false,
            outputs_right_only: false,
            introduces_mark_column: false,
        }
    );

    let JoinIslandNode::Join { inner, .. } = island.root() else {
        panic!("root should be a join");
    };
    let semi_atom = inner.atom().expect("inner child should be a join");
    assert_eq!(semi_atom.join_type, JoinType::LeftSemi);

    Ok(())
}

#[test]
fn test_join_ordering_pass_reorders_inner_region_by_cost() -> crate::error::Result<()> {
    let ctx = numbered_ctx(&[10_000, 10, 10]);
    let t0 = ctx.logical_get(TableRef::bare("t0"), None)?.build();
    let t1 = ctx.logical_get(TableRef::bare("t1"), None)?.build();
    let t2 = ctx.logical_get(TableRef::bare("t2"), None)?.build();

    let t0_v1 = ctx.col(Some(&TableRef::bare("t0")), "t0.v1")?;
    let t1_v1 = ctx.col(Some(&TableRef::bare("t1")), "t1.v1")?;
    let t2_v1 = ctx.col(Some(&TableRef::bare("t2")), "t2.v1")?;

    let root = t0
        .clone()
        .with_ctx(&ctx)
        .logical_join(t1.clone(), column_ref(t0_v1).eq(column_ref(t1_v1)), JoinType::Inner)
        .build()
        .with_ctx(&ctx)
        .logical_join(t2, column_ref(t1_v1).eq(column_ref(t2_v1)), JoinType::Inner)
        .build();

    let pass = JoinOrderingPass::new();
    let optimized = pass.apply(root.clone(), &ctx)?;

    assert_ne!(quick_explain(&root, &ctx), quick_explain(&optimized, &ctx));
    assert!(ctx.cm.compute_total_cost(optimized.as_ref(), &ctx)? < ctx.cm.compute_total_cost(root.as_ref(), &ctx)?);

    let optimized_join = optimized.try_borrow::<Join>().unwrap();
    assert_eq!(optimized_join.join_type(), &JoinType::Inner);
    assert!(optimized_join.implementation().is_some());
    assert!(has_direct_join_pair(optimized.as_ref(), &ctx, "t1", "t2"));

    Ok(())
}

#[test]
fn test_join_ordering_pass_optimizes_inner_child_under_left_outer_join()
-> crate::error::Result<()> {
    let ctx = numbered_ctx(&[10_000, 10, 10, 1_000]);
    let t0 = ctx.logical_get(TableRef::bare("t0"), None)?.build();
    let t1 = ctx.logical_get(TableRef::bare("t1"), None)?.build();
    let t2 = ctx.logical_get(TableRef::bare("t2"), None)?.build();
    let t3 = ctx.logical_get(TableRef::bare("t3"), None)?.build();

    let t0_v1 = ctx.col(Some(&TableRef::bare("t0")), "t0.v1")?;
    let t1_v1 = ctx.col(Some(&TableRef::bare("t1")), "t1.v1")?;
    let t2_v1 = ctx.col(Some(&TableRef::bare("t2")), "t2.v1")?;
    let t3_v1 = ctx.col(Some(&TableRef::bare("t3")), "t3.v1")?;

    let inner_region = t0
        .clone()
        .with_ctx(&ctx)
        .logical_join(t1.clone(), column_ref(t0_v1).eq(column_ref(t1_v1)), JoinType::Inner)
        .build()
        .with_ctx(&ctx)
        .logical_join(t2, column_ref(t1_v1).eq(column_ref(t2_v1)), JoinType::Inner)
        .build();

    let root = inner_region.with_ctx(&ctx).logical_join(
        t3,
        column_ref(t1_v1).eq(column_ref(t3_v1)),
        JoinType::LeftOuter,
    )
    .build();

    let optimized = JoinOrderingPass::new().apply(root, &ctx)?;

    let root_join = optimized.try_borrow::<Join>().unwrap();
    assert_eq!(root_join.join_type(), &JoinType::LeftOuter);
    assert_eq!(root_join.implementation(), &None);
    assert!(has_direct_join_pair(root_join.outer().as_ref(), &ctx, "t1", "t2"));

    let optimized_inner = root_join.outer().try_borrow::<Join>().unwrap();
    assert_eq!(optimized_inner.join_type(), &JoinType::Inner);
    assert!(matches!(
        optimized_inner.implementation(),
        Some(JoinImplementation::NestedLoop) | Some(JoinImplementation::Hash { .. })
    ));

    Ok(())
}

fn numbered_ctx(rows: &[usize]) -> IRContext {
    IRContext::with_numbered_tables(
        rows.iter()
            .copied()
            .map(|row_count| TableStatistics {
                row_count,
                column_statistics: Default::default(),
                size_bytes: None,
            })
            .collect(),
        1,
    )
}

fn leaf_table_name(op: &crate::ir::Operator, ctx: &IRContext) -> crate::error::Result<String> {
    if let Ok(get) = op.try_borrow::<Get>() {
        return Ok(ctx.get_binding(get.table_index())?.table_ref().table().to_string());
    }
    if let Ok(join) = op.try_borrow::<Join>() {
        let outer = leaf_table_name(join.outer().as_ref(), ctx)?;
        let inner = leaf_table_name(join.inner().as_ref(), ctx)?;
        return Ok(format!("({outer},{inner})"));
    }
    if let Ok(select) = op.try_borrow::<crate::ir::operator::Select>() {
        return leaf_table_name(select.input().as_ref(), ctx);
    }
    panic!("unexpected leaf operator: {:?}", op.kind);
}

fn has_direct_join_pair(op: &crate::ir::Operator, ctx: &IRContext, a: &str, b: &str) -> bool {
    let Ok(join) = op.try_borrow::<Join>() else {
        return false;
    };

    let outer_leaf = leaf_table_name(join.outer().as_ref(), ctx);
    let inner_leaf = leaf_table_name(join.inner().as_ref(), ctx);
    if let (Ok(outer_leaf), Ok(inner_leaf)) = (outer_leaf, inner_leaf)
        && ((outer_leaf == a && inner_leaf == b) || (outer_leaf == b && inner_leaf == a))
    {
        return true;
    }

    has_direct_join_pair(join.outer().as_ref(), ctx, a, b)
        || has_direct_join_pair(join.inner().as_ref(), ctx, a, b)
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
    assert!(dphyp.solve(&hypergraph), "DPHyp should find a plan on JOB Query 13d");

    let dphyp_pairs = dphyp
        .get_pairs()
        .iter()
        .map(|pair| normalize_pair(&pair.left, &pair.right))
        .collect::<BTreeSet<_>>();

    assert_eq!(dpccp_pairs, dphyp_pairs);
}
