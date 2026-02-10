use crate::ir::builder::{boolean, column_assign, column_ref, mock_scan_with_columns};
use crate::ir::convert::IntoScalar;
use crate::ir::operator::{Operator, join::JoinType};
use crate::ir::scalar::{BinaryOp, BinaryOpKind};
use crate::ir::{Column, DataType, IRContext};
use crate::rules::decorrelation::UnnestingRule;
use std::sync::Arc;

// --- Helper functions to help create a logical plan ---

fn make_cols(ctx: &IRContext, n: usize) -> Vec<Column> {
    (0..n)
        .map(|_| ctx.define_column(DataType::Int32, None))
        .collect()
}

/// Creates a NULL safe equality predicate
fn null_safe_eq(
    lhs: Arc<crate::ir::Scalar>,
    rhs: Arc<crate::ir::Scalar>,
) -> Arc<crate::ir::Scalar> {
    BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, lhs, rhs).into_scalar()
}

/// Creates a domain operator and remaps its output columns to new aliases.
fn create_domain_with_aliases(
    input: Arc<Operator>,
    from_cols: Vec<Column>,
    to_cols: Vec<Column>,
) -> Arc<Operator> {
    assert_eq!(from_cols.len(), to_cols.len(), "domain remap must be 1:1");
    let project_exprs: Vec<_> = from_cols.iter().map(|c| column_ref(*c)).collect();
    let domain_project = input.logical_project(project_exprs);

    let group_keys: Vec<_> = from_cols
        .iter()
        .map(|c| column_assign(*c, column_ref(*c)))
        .collect();
    let domain_distinct = domain_project.logical_aggregate(std::iter::empty(), group_keys);

    let remap_assigns: Vec<_> = from_cols
        .iter()
        .zip(to_cols.iter())
        .map(|(from, to)| column_assign(*to, column_ref(*from)))
        .collect();
    domain_distinct.logical_remap(remap_assigns)
}

// --- Helper functions to test the rule ---

/// Applies the unnesting rule and asserts the result matches expected.
fn assert_unnesting(ctx: &IRContext, input: Arc<Operator>, expected: Arc<Operator>) {
    let res = UnnestingRule::new().apply(input, &ctx);
    assert_eq!(res, expected, "Unnested plan does not match expected plan");
}

// --- Tests ---

/// Test Simple Unnesting: A very simple seelct pull-up
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
/// |--- Scan(T2)
#[test]
fn test_simple_unnesting() {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0).logical_select(column_ref(t2).eq(column_ref(t1)));

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);

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
///      |--- Select [T3.v3 = T2.v2 && T3.v3 = T1.v1]
///           |--- Scan(T3)
///
/// OUTPUT:
/// Join [T1.v1 IS NOT DISTINCT FROM d1]
/// |--- Scan(T1)
/// |--- Join [T2.v2 IS NOT DISTINCT FROM T3.v3]
///      |--- Scan(T2)
///      |--- Join [T3.v3 = d1]
///           |--- Domain(T1.v1) -> d1
///           |--- Scan(T3)
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
            let right_branch = mock_scan_with_columns(3, t3_cols.to_vec(), 100.0)
                .logical_select(column_ref(t3).eq(column_ref(t2)).and(column_ref(t3).eq(column_ref(t1))));

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

        let left_branch = mock_scan_with_columns(1, t1_cols.to_vec(), 100.0);
        let right_branch = {
            let left_branch = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);
            let right_branch = {
                let left_branch =
                    create_domain_with_aliases(mock_scan_with_columns(1, t1_cols.to_vec(), 100.0), vec![t1], vec![d1]);
                let right_branch = mock_scan_with_columns(3, t3_cols.to_vec(), 100.0);

                left_branch.logical_join(
                    right_branch,
                    column_ref(t3).eq(column_ref(d1)),
                    JoinType::Inner,
                )
            };

            left_branch.logical_join(
                right_branch,
                null_safe_eq(column_ref(t2), column_ref(t3)),
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
/// Join [T1.v1 IS NOT DISTINCT FROM T2.v2]
/// |--- Scan(T1)
/// |--- Join [d1 IS NOT DISTINCT FROM T2.v2]
///      |--- Domain(T1, T1.v1) -> d1
///      |--- Aggregate [keys: T2.v2 := T2.v2]
///           |--- Scan(T2)
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
            let agg_input = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0).logical_select(column_ref(t2).eq(column_ref(t1)));
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
            let domain =
                create_domain_with_aliases(mock_scan_with_columns(1, t1_cols.to_vec(), 100.0), vec![t1], vec![d1]);
            let agg_input = mock_scan_with_columns(2, t2_cols.to_vec(), 100.0);
            let keys = vec![column_assign(t2, column_ref(t2))];
            let agg = agg_input.logical_aggregate(std::iter::empty(), keys);
            domain.logical_join(agg, null_safe_eq(column_ref(d1), column_ref(t2)), JoinType::Left)
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(t2)),
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
///      |--- Scan(T2)
///      |--- Scan(T3)
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
                ;

            let right_sel = mock_scan_with_columns(3, t3_cols.clone(), 100.0)
                ;

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
    ) -> (Arc<Operator>, Vec<Column>, Vec<Column>, Column, Column, Column, Column) {
        let t1_cols = make_cols(ctx, 2);
        let t2_cols = make_cols(ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t2c1 = t2_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let select_input = mock_scan_with_columns(2, t2_cols.clone(), 100.0)
                .logical_select(column_ref(t2c0).gt(column_ref(t1c0)).and(column_ref(t2c1).gt(column_ref(t1c1))));
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
                column_ref(t2c0).gt(column_ref(d1)).and(column_ref(t2c1).gt(column_ref(d2))),
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
/// Join [T1.v0 IS NOT DISTINCT FROM d1 && T1.v1 IS NOT DISTINCT FROM d2]
/// |--- Scan(T1) [v0, v1]
/// |--- Join [T2.v2 IS NOT DISTINCT FROM d3]
///      |--- Scan(T2) [v2]
///      |--- Join [T3.v3 IS NOT DISTINCT FROM T4.v4]
///           |--- Scan(T3) [v3]
///           |--- Join [T4.v4 = d3 && T4.v4 = d1 && T4.v5 = d2]
///                |--- Join [true]
///                |    |--- Domain(T2.v2) -> d3
///                |    |--- Domain(T1.v0, T1.v1) -> (d1, d2)
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
        let mut next_id = ctx.column_meta.lock().unwrap().len();
        let mut fresh = || {
            let id = next_id;
            next_id += 1;
            Column(id)
        };
        let d1 = fresh();
        let d2 = fresh();
        let d3 = fresh();

        let left_branch = mock_scan_with_columns(1, t1_cols.clone(), 100.0);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols.clone(), 100.0);
            let mid_right = {
                let inner_left = mock_scan_with_columns(3, t3_cols.clone(), 100.0);
                let inner_right = {
                    let local_domain = create_domain_with_aliases(
                        mock_scan_with_columns(2, t2_cols.clone(), 100.0),
                        vec![t2c0],
                        vec![d3],
                    );
                    let parent_domain = create_domain_with_aliases(
                        mock_scan_with_columns(1, t1_cols.clone(), 100.0),
                        vec![t1c0, t1c1],
                        vec![d1, d2],
                    );
                    local_domain
                        .logical_join(parent_domain, boolean(true), JoinType::Inner)
                        .logical_join(
                            mock_scan_with_columns(4, t4_cols.clone(), 100.0),
                            column_ref(t4c0)
                                .eq(column_ref(d3))
                                .and(column_ref(t4c0).eq(column_ref(d1)))
                                .and(column_ref(t4c1).eq(column_ref(d2))),
                            JoinType::Inner,
                        )
                };
                inner_left.logical_join(
                    inner_right,
                    null_safe_eq(column_ref(t3c0), column_ref(t4c0)),
                    JoinType::Inner,
                )
            };
            mid_left.logical_join(
                mid_right,
                null_safe_eq(column_ref(t2c0), column_ref(d3)),
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
