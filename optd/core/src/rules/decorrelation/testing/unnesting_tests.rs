use super::helpers::{assert_unnesting, create_domain_with_aliases, make_cols, null_safe_eq};
use crate::ir::builder::{boolean, column_assign, column_ref, mock_scan_with_columns};
use crate::ir::convert::IntoOperator;
use crate::ir::explain::quick_explain;
use crate::ir::operator::{LogicalJoin, LogicalOrderBy, Operator, OperatorKind, join::JoinType};
use crate::ir::properties::TupleOrderingDirection;
use crate::ir::{Column, DataType, IRContext};
use crate::rules::decorrelation::UnnestingRule;
use std::sync::Arc;

/// Test Simple Unnesting: A very simple select pull-up
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1)
/// |--- Select [T2.v2 = T1.v1]
///      |--- Scan(T2)
///
/// OUTPUT:
/// Join [T1.v1 IS NOT DISTINCT FROM T2.v2]
/// |--- Scan(T1)
/// |--- Select [T2.v2 = T2.v2]
///      |--- Scan(T2)
#[test]
fn test_simple_unnesting() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0)
            .logical_select(column_ref(t2).eq(column_ref(t1)));

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0)
            .logical_select(column_ref(t2).eq(column_ref(t2)));

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(t2)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Nesting & Scope: 2 nested dependent joins
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1)
/// |--- DependentJoin
///      |--- Scan(T2)
///      |--- Select [T3.v2 = T2.v1 && T3.v2 > T1.v0]
///           |--- Scan(T3)
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM d1]
/// |--- Scan(T1) [v0]
/// |--- Join [T2.v1 IS NOT DISTINCT FROM d2]
///      |--- Scan(T2) [v1]
///      |--- Join [T3.v2 = d2 && T3.v2 > d1]
///           |--- Join [true]
///           |    |--- Domain(T2.v1) -> d2
///           |    |--- Domain(T1.v0) -> d1
///           |--- Scan(T3) [v2]
#[test]
fn test_deep_nesting_and_scope() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t3_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];
    let t3 = t3_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let left_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);
            let right_branch = mock_scan_with_columns(3, t3_cols.to_vec(), 100.0).logical_select(
                column_ref(t3)
                    .eq(column_ref(t2))
                    .and(column_ref(t3).gt(column_ref(t1))),
            );

            left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();
        let d2 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let left_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);
            let right_branch = {
                let local_domain = create_domain_with_aliases(
                    mock_scan_with_columns(2, t2_cols.to_vec(), 100.0),
                    vec![t2],
                    vec![d2],
                );
                let parent_domain = create_domain_with_aliases(
                    mock_scan_with_columns(1, t1_cols.to_vec(), 100.0),
                    vec![t1],
                    vec![d1],
                );
                let t3_scan = mock_scan_with_columns(3, t3_cols.to_vec(), 100.0);
                local_domain
                    .logical_join(parent_domain, boolean(true), JoinType::Inner)
                    .logical_join(
                        t3_scan,
                        column_ref(t3)
                            .eq(column_ref(d2))
                            .and(column_ref(t3).gt(column_ref(d1))),
                        JoinType::Inner,
                    )
            };

            left_branch.logical_join(
                right_branch,
                null_safe_eq(column_ref(t2), column_ref(d2)),
                JoinType::Inner,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Nested Dependent Joins with Intermediate Outer Ref Access:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0]
/// |--- Select [T2.v1 > T1.v0]
///      |--- DependentJoin
///           |--- Scan(T2) [v1]
///           |--- Select [T3.v2 = T2.v1]
///                |--- Scan(T3) [v2]
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM d1]
/// |--- Scan(T1) [v0]
/// |--- Join [T2.v1 > d1]
///      |--- Domain(T1.v0) -> d1
///      |--- Join [T2.v1 IS NOT DISTINCT FROM T3.v2]
///           |--- Scan(T2) [v1]
///           |--- Select [T3.v2 = T3.v2]
///                |--- Scan(T3) [v2]
#[test]
fn test_nested_dep_join_intermediate_select_preserves_outer_ref() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t3_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];
    let t3 = t3_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let nested_dep = {
                let left_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);
                let right_branch = mock_scan_with_columns(3, t3_cols.to_vec(), 100.0)
                    .logical_select(column_ref(t3).eq(column_ref(t2)));
                left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
            };
            nested_dep.logical_select(column_ref(t2).gt(column_ref(t1)))
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let domain = create_domain_with_aliases(
                mock_scan_with_columns(1, t1_cols.to_vec(), 100.0),
                vec![t1],
                vec![d1],
            );
            let nested_join = {
                let left_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);
                let right_branch = mock_scan_with_columns(3, t3_cols.to_vec(), 100.0)
                    .logical_select(column_ref(t3).eq(column_ref(t3)));
                left_branch.logical_join(
                    right_branch,
                    null_safe_eq(column_ref(t2), column_ref(t3)),
                    JoinType::Inner,
                )
            };
            domain.logical_join(
                nested_join,
                column_ref(t2).gt(column_ref(d1)),
                JoinType::Inner,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Complex Operators: Aggregates + Domain
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1)
/// |--- Aggregate []
///      |--- Select [T2.v2 = T1.v1]
///           |--- Scan(T2)
///
/// OUTPUT:
/// Join [T1.v1 IS NOT DISTINCT FROM d1]
/// |--- Scan(T1)
/// |--- Join [d1 IS NOT DISTINCT FROM T2.v2]
///      |--- Domain(T1, T1.v1) -> d1
///      |--- Aggregate [keys: T2.v2 := T2.v2]
///           |--- Select [T2.v2 = T2.v2]
///                |--- Scan(T2)
#[test]
fn test_complex_operators() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let agg_input = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0)
                .logical_select(column_ref(t2).eq(column_ref(t1)));
            agg_input.logical_aggregate(std::iter::empty(), std::iter::empty())
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let domain = create_domain_with_aliases(
                mock_scan_with_columns(1, t1_cols.to_vec(), 100.0),
                vec![t1],
                vec![d1],
            );
            let agg_input = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0)
                .logical_select(column_ref(t2).eq(column_ref(t2)));
            let keys = vec![column_assign(t2, column_ref(t2))];
            let agg = agg_input.logical_aggregate(std::iter::empty(), keys);
            domain.logical_join(
                agg,
                null_safe_eq(column_ref(d1), column_ref(t2)),
                JoinType::Left,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Join Equivalence Merging: Decorrelate without domain joins
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1)
/// |--- Join [true]
///      |--- Select [T2.v1 = T1.v0]
///      |    |--- Scan(T2)
///      |--- Select [T3.v2 = T1.v0]
///           |--- Scan(T3)
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM T2.v1]
/// |--- Scan(T1)
/// |--- Join [T2.v1 IS NOT DISTINCT FROM T3.v2]
///      |--- Select [T2.v1 = T2.v1]
///      |    |--- Scan(T2)
///      |--- Select [T3.v2 = T3.v2]
///           |--- Scan(T3)
#[test]
fn test_join_equivalence_merging() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t3_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];
    let t3 = t3_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let left_sel = mock_scan_with_columns(2, t2_cols.clone(), 100.0)
                .logical_select(column_ref(t2).eq(column_ref(t1)));
            let right_sel = mock_scan_with_columns(3, t3_cols.clone(), 100.0)
                .logical_select(column_ref(t3).eq(column_ref(t1)));

            left_sel.logical_join(right_sel, boolean(true), JoinType::Inner)
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let left_sel = mock_scan_with_columns(2, t2_cols.clone(), 100.0)
                .logical_select(column_ref(t2).eq(column_ref(t2)));

            let right_sel = mock_scan_with_columns(3, t3_cols.clone(), 100.0)
                .logical_select(column_ref(t3).eq(column_ref(t3)));

            left_sel.logical_join(
                right_sel,
                null_safe_eq(column_ref(t2), column_ref(t3)),
                JoinType::Inner,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(t2)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Multi-Outer Refs with Non-Equi Predicate (Domain Retained):
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0, v1]
/// |--- Aggregate [keys: T2.v2 := T2.v2]
///      |--- Select [T2.v2 > T1.v0 && T2.v3 > T1.v1]
///           |--- Scan(T2) [v2, v3]
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM d1 && T1.v1 IS NOT DISTINCT FROM d2]
/// |--- Scan(T1) [v0, v1]
/// |--- Aggregate [keys: T2.v2 := T2.v2, d1 := d1, d2 := d2]
///      |--- Join [T2.v2 > d1 && T2.v3 > d2]
///           |--- Domain(T1.v0, T1.v1) -> d1, d2
///           |--- Scan(T2) [v2, v3]
#[test]
fn test_multi_outer_refs_domain_retained() {
    fn build_input_plan(
        ctx: &IRContext,
    ) -> (
        Arc<Operator>,
        Vec<Column>,
        Vec<Column>,
        Column,
        Column,
        Column,
        Column,
    ) {
        let t1_cols = make_cols(ctx, 2);
        let t2_cols = make_cols(ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t2c1 = t2_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let select_input = mock_scan_with_columns(2, t2_cols.clone(), 100.0).logical_select(
                column_ref(t2c0)
                    .gt(column_ref(t1c0))
                    .and(column_ref(t2c1).gt(column_ref(t1c1))),
            );
            let keys = vec![column_assign(t2c0, column_ref(t2c0))];
            select_input.logical_aggregate(std::iter::empty(), keys)
        };
        (
            left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner),
            t1_cols,
            t2_cols,
            t1c0,
            t1c1,
            t2c0,
            t2c1,
        )
    }

    let ctx = IRContext::with_empty_magic();
    let (input_plan, t1_cols, t2_cols, t1c0, t1c1, t2c0, t2c1) = build_input_plan(&ctx);
    let expected_plan = {
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();
        let d2 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let domain = create_domain_with_aliases(
                mock_scan_with_columns(1, t1_cols.clone(), 100.0),
                vec![t1c0, t1c1],
                vec![d1, d2],
            );
            let join_input = domain.logical_join(
                mock_scan_with_columns(2, t2_cols.clone(), 100.0),
                column_ref(t2c0)
                    .gt(column_ref(d1))
                    .and(column_ref(t2c1).gt(column_ref(d2))),
                JoinType::Inner,
            );
            let keys = vec![
                column_assign(t2c0, column_ref(t2c0)),
                column_assign(d1, column_ref(d1)),
                column_assign(d2, column_ref(d2)),
            ];
            join_input.logical_aggregate(std::iter::empty(), keys)
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(d1))
                .and(null_safe_eq(column_ref(t1c1), column_ref(d2))),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Deep Nesting with Multiple Dependencies:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0, v1]
/// |--- DependentJoin
///      |--- Scan(T2) [v2]
///      |--- DependentJoin
///           |--- Scan(T3) [v3]
///           |--- Select [T4.v4 = T3.v3 && T4.v4 = T2.v2 && T4.v4 = T1.v0 && T4.v5 = T1.v1]
///                |--- Scan(T4) [v4, v5]
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM T4.v4 && T1.v1 IS NOT DISTINCT FROM T4.v5]
/// |--- Scan(T1) [v0, v1]
/// |--- Join [T2.v2 IS NOT DISTINCT FROM T4.v4]
///      |--- Scan(T2) [v2]
///      |--- Join [T3.v3 IS NOT DISTINCT FROM T4.v4]
///           |--- Scan(T3) [v3]
///           |--- Select [T4.v4 = T4.v4 && T4.v4 = T4.v4 && T4.v4 = T4.v4 && T4.v5 = T4.v5]
///                |--- Scan(T4) [v4, v5]
#[test]
fn test_deep_nesting_multiple_dependencies() {
    #[allow(clippy::type_complexity)]
    fn build_input_plan(
        ctx: &IRContext,
    ) -> (
        Arc<Operator>,
        Vec<Column>,
        Vec<Column>,
        Vec<Column>,
        Vec<Column>,
        Column,
        Column,
        Column,
        Column,
        Column,
        Column,
    ) {
        let t1_cols = make_cols(ctx, 2);
        let t2_cols = make_cols(ctx, 1);
        let t3_cols = make_cols(ctx, 1);
        let t4_cols = make_cols(ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];
        let t4c0 = t4_cols[0];
        let t4c1 = t4_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols.clone(), 100.0);
            let mid_right = {
                let inner_left = mock_scan_with_columns(3, t3_cols.clone(), 100.0);
                let inner_right = {
                    let select_input = mock_scan_with_columns(4, t4_cols.clone(), 100.0);
                    select_input.logical_select(
                        column_ref(t4c0)
                            .eq(column_ref(t3c0))
                            .and(column_ref(t4c0).eq(column_ref(t2c0)))
                            .and(column_ref(t4c0).eq(column_ref(t1c0)))
                            .and(column_ref(t4c1).eq(column_ref(t1c1))),
                    )
                };
                inner_left.logical_dependent_join(inner_right, boolean(true), JoinType::Inner)
            };
            mid_left.logical_dependent_join(mid_right, boolean(true), JoinType::Inner)
        };

        (
            left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner),
            t1_cols,
            t2_cols,
            t3_cols,
            t4_cols,
            t1c0,
            t1c1,
            t2c0,
            t3c0,
            t4c0,
            t4c1,
        )
    }

    let ctx = IRContext::with_empty_magic();
    let (input_plan, t1_cols, t2_cols, t3_cols, t4_cols, t1c0, t1c1, t2c0, t3c0, t4c0, t4c1) =
        build_input_plan(&ctx);
    let expected_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols.clone(), 100.0);
            let mid_right = {
                let inner_left = mock_scan_with_columns(3, t3_cols.clone(), 100.0);
                let inner_right = mock_scan_with_columns(4, t4_cols.clone(), 100.0).logical_select(
                    column_ref(t4c0)
                        .eq(column_ref(t4c0))
                        .and(column_ref(t4c0).eq(column_ref(t4c0)))
                        .and(column_ref(t4c0).eq(column_ref(t4c0)))
                        .and(column_ref(t4c1).eq(column_ref(t4c1))),
                );
                inner_left.logical_join(
                    inner_right,
                    null_safe_eq(column_ref(t3c0), column_ref(t4c0)),
                    JoinType::Inner,
                )
            };
            mid_left.logical_join(
                mid_right,
                null_safe_eq(column_ref(t2c0), column_ref(t4c0)),
                JoinType::Inner,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(t4c0))
                .and(null_safe_eq(column_ref(t1c1), column_ref(t4c1))),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Nested Dependent Joins + Regular Join + OrderBy + Map:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0]
/// |--- DependentJoin
///      |--- Scan(T2) [v1]
///      |--- Join [T3.v3 = T4.v5]
///           |--- OrderBy [T3.v2 ASC]
///           |    |--- Project [T3.v2, T3.v3]
///           |         |--- Select [T3.v2 > T2.v1 && T3.v3 > T1.v0]
///           |              |--- Scan(T3) [v2, v3]
///           |--- Project [r0 := T4.v4, T4.v5]
///                |--- Select [T4.v4 = T2.v1]
///                     |--- Scan(T4) [v4, v5]
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM d1]
/// |--- Scan(T1) [v0]
/// |--- Join [T2.v1 IS NOT DISTINCT FROM T4.v4]
///      |--- Scan(T2) [v1]
///      |--- Join [T3.v3 = T4.v5 && d2 IS NOT DISTINCT FROM T4.v4]
///           |--- OrderBy [T3.v2 ASC]
///           |    |--- Project [T3.v2, T3.v3]
///           |         |--- Join [T3.v2 > d2 && T3.v3 > d1]
///           |              |--- Join [true]
///           |              |    |--- Domain(T2.v1) -> d2
///           |              |    |--- Domain(T1.v0) -> d1
///           |              |--- Scan(T3) [v2, v3]
///           |--- Project [r0 := T4.v4, T4.v5, T4.v4]
///                |--- Select [T4.v4 = T4.v4]
///                     |--- Scan(T4) [v4, v5]
#[test]
fn test_nested_dep_join_regular_join_orderby_project_domain_vs_repr() {
    #[allow(clippy::type_complexity)]
    fn build_input_plan(
        ctx: &IRContext,
    ) -> (
        Arc<Operator>,
        Vec<Column>,
        Vec<Column>,
        Vec<Column>,
        Vec<Column>,
        Column,
        Column,
        Column,
        Column,
        Column,
        Column,
        Column,
    ) {
        let t1_cols = make_cols(ctx, 1);
        let t2_cols = make_cols(ctx, 1);
        let t3_cols = make_cols(ctx, 2);
        let t4_cols = make_cols(ctx, 2);
        let map_col = ctx.define_column(DataType::Int32, None);

        let t1c0 = t1_cols[0];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];
        let t3c1 = t3_cols[1];
        let t4c0 = t4_cols[0];
        let t4c1 = t4_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols.clone(), 100.0);
            let mid_right = {
                let left_regular = {
                    let select_input = mock_scan_with_columns(3, t3_cols.clone(), 100.0)
                        .logical_select(
                            column_ref(t3c0)
                                .gt(column_ref(t2c0))
                                .and(column_ref(t3c1).gt(column_ref(t1c0))),
                        )
                        .logical_project(vec![column_ref(t3c0), column_ref(t3c1)]);
                    LogicalOrderBy::new(
                        select_input,
                        vec![(column_ref(t3c0), TupleOrderingDirection::Asc)],
                    )
                    .into_operator()
                };
                let right_regular = mock_scan_with_columns(4, t4_cols.clone(), 100.0)
                    .logical_select(column_ref(t4c0).eq(column_ref(t2c0)))
                    .logical_project(vec![
                        column_assign(map_col, column_ref(t4c0)),
                        column_ref(t4c1),
                    ]);
                left_regular.logical_join(
                    right_regular,
                    column_ref(t3c1).eq(column_ref(t4c1)),
                    JoinType::Inner,
                )
            };
            mid_left.logical_dependent_join(mid_right, boolean(true), JoinType::Inner)
        };

        (
            left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner),
            t1_cols,
            t2_cols,
            t3_cols,
            t4_cols,
            map_col,
            t1c0,
            t2c0,
            t3c0,
            t3c1,
            t4c0,
            t4c1,
        )
    }

    let ctx = IRContext::with_empty_magic();
    let (
        input_plan,
        t1_cols,
        t2_cols,
        t3_cols,
        t4_cols,
        map_col,
        t1c0,
        t2c0,
        t3c0,
        t3c1,
        t4c0,
        t4c1,
    ) = build_input_plan(&ctx);
    let expected_plan = {
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();
        let d2 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols.clone(), 100.0);
            let mid_right = {
                let left_regular = {
                    let current_domain = create_domain_with_aliases(
                        mock_scan_with_columns(2, t2_cols.clone(), 100.0),
                        vec![t2c0],
                        vec![d2],
                    );
                    let parent_domain = create_domain_with_aliases(
                        mock_scan_with_columns(1, t1_cols.clone(), 100.0),
                        vec![t1c0],
                        vec![d1],
                    );
                    let select_input = current_domain
                        .logical_join(parent_domain, boolean(true), JoinType::Inner)
                        .logical_join(
                            mock_scan_with_columns(3, t3_cols.clone(), 100.0),
                            column_ref(t3c0)
                                .gt(column_ref(d2))
                                .and(column_ref(t3c1).gt(column_ref(d1))),
                            JoinType::Inner,
                        )
                        .logical_project(vec![column_ref(t3c0), column_ref(t3c1)]);
                    LogicalOrderBy::new(
                        select_input,
                        vec![(column_ref(t3c0), TupleOrderingDirection::Asc)],
                    )
                    .into_operator()
                };
                let right_regular = mock_scan_with_columns(4, t4_cols.clone(), 100.0)
                    .logical_select(column_ref(t4c0).eq(column_ref(t4c0)))
                    .logical_project(vec![
                        column_assign(map_col, column_ref(t4c0)),
                        column_ref(t4c1),
                        column_ref(t4c0),
                    ]);
                left_regular.logical_join(
                    right_regular,
                    column_ref(t3c1)
                        .eq(column_ref(t4c1))
                        .and(null_safe_eq(column_ref(d2), column_ref(t4c0))),
                    JoinType::Inner,
                )
            };
            mid_left.logical_join(
                mid_right,
                null_safe_eq(column_ref(t2c0), column_ref(t4c0)),
                JoinType::Inner,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}

/// Test Left Join Right-Side Equivalence Is Not Used As Outer Representative:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0]
/// |--- Join [true] (Left)
///      |--- Scan(T2) [v1]
///      |--- Select [T3.v2 = T1.v0]
///           |--- Scan(T3) [v2]
///
/// EXPECTATION:
/// The top-level decorrelated join condition must not use T3.v2 as the
/// representative for T1.v0, because T3.v2 is on the non-preserved side of
/// a left join and can be NULL-extended.
#[test]
fn test_left_join_right_cclass_not_propagated() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t3_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];
    let t3 = t3_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let left_regular = mock_scan_with_columns(2, t2_cols.clone(), 100.0);
            let right_regular = mock_scan_with_columns(3, t3_cols.clone(), 100.0)
                .logical_select(column_ref(t3).eq(column_ref(t1)));
            left_regular.logical_join(right_regular, boolean(true), JoinType::Left)
        };
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let res = UnnestingRule::new().apply(input_plan, &ctx);
    let root_join = match &res.kind {
        OperatorKind::LogicalJoin(meta) => LogicalJoin::borrow_raw_parts(meta, &res.common),
        _ => panic!(
            "expected decorrelated root to be a join, got:\n{}",
            quick_explain(&res, &ctx)
        ),
    };
    let used_cols = root_join.join_cond().used_columns();
    assert!(
        used_cols.contains(&t1),
        "top join condition should reference outer column; plan:\n{}",
        quick_explain(&res, &ctx)
    );
    assert!(
        !used_cols.contains(&t3),
        "top join condition must not use nullable right-side left-join column as outer repr; plan:\n{}",
        quick_explain(&res, &ctx)
    );
    assert!(
        used_cols.iter().any(|c| *c != t1 && *c != t2 && *c != t3),
        "expected a domain representative column in top join condition; plan:\n{}",
        quick_explain(&res, &ctx)
    );
}

/// Test Partial Representative Resolution Is All-Or-Nothing:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0, v1]
/// |--- Select [T2.v2 = T1.v0 && T2.v3 > T1.v1]
///      |--- Scan(T2) [v2, v3]
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM d1 && T1.v1 IS NOT DISTINCT FROM d2]
/// |--- Scan(T1) [v0, v1]
/// |--- Join [T2.v2 = d1 && T2.v3 > d2]
///      |--- Domain(T1.v0, T1.v1) -> d1, d2
///      |--- Scan(T2) [v2, v3]
#[test]
fn test_partial_repr_resolution_all_or_nothing() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 2);
    let t2_cols = make_cols(&ctx, 2);
    let t1c0 = t1_cols[0];
    let t1c1 = t1_cols[1];
    let t2c0 = t2_cols[0];
    let t2c1 = t2_cols[1];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = mock_scan_with_columns(2, t2_cols.clone(), 100.0).logical_select(
            column_ref(t2c0)
                .eq(column_ref(t1c0))
                .and(column_ref(t2c1).gt(column_ref(t1c1))),
        );
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();
        let d2 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let domain = create_domain_with_aliases(
                mock_scan_with_columns(1, t1_cols.clone(), 100.0),
                vec![t1c0, t1c1],
                vec![d1, d2],
            );
            domain.logical_join(
                mock_scan_with_columns(2, t2_cols.clone(), 100.0),
                column_ref(t2c0)
                    .eq(column_ref(d1))
                    .and(column_ref(t2c1).gt(column_ref(d2))),
                JoinType::Inner,
            )
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(d1))
                .and(null_safe_eq(column_ref(t1c1), column_ref(d2))),
            JoinType::Inner,
        )
    };

    assert_unnesting(&ctx, input_plan, expected_plan);
}
