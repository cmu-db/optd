use super::helpers::{
    TestOperatorExt, aggregate_with_outputs, assert_unnesting, create_domain_with_aliases,
    make_cols, make_mark_col, mock_scan_with_columns, null_safe_eq, project_with_outputs,
};
use crate::error::Result;
use crate::ir::IRContext;
use crate::ir::builder::{boolean, column_ref};
use crate::ir::convert::IntoOperator;
use crate::ir::explain::quick_explain;
use crate::ir::operator::{Join, OperatorKind, OrderBy, join::JoinType};
use crate::ir::properties::TupleOrderingDirection;
use crate::rules::decorrelation::UnnestingRule;

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
fn test_simple_unnesting() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch =
            mock_scan_with_columns(2, t2_cols).logical_select(column_ref(t2).eq(column_ref(t1)));
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch =
            mock_scan_with_columns(2, t2_cols).logical_select(column_ref(t2).eq(column_ref(t2)));
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(t2)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
fn test_deep_nesting_and_scope() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let t3_cols = make_cols(&input_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let t3 = t3_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let left_branch = mock_scan_with_columns(2, t2_cols);
            let right_branch = mock_scan_with_columns(3, t3_cols).logical_select(
                column_ref(t3)
                    .eq(column_ref(t2))
                    .and(column_ref(t3).gt(column_ref(t1))),
            );
            left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t3_cols = make_cols(&expected_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let t3 = t3_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone());
        let right_branch = {
            let left_branch = mock_scan_with_columns(2, t2_cols.clone());
            let right_branch = {
                let (local_domain, local_outputs) = create_domain_with_aliases(
                    &expected_ctx,
                    mock_scan_with_columns(2, t2_cols),
                    vec![t2],
                )?;
                let d2 = local_outputs[0];
                let (parent_domain, parent_outputs) = create_domain_with_aliases(
                    &expected_ctx,
                    mock_scan_with_columns(1, t1_cols),
                    vec![t1],
                )?;
                let d1 = parent_outputs[0];
                let t3_scan = mock_scan_with_columns(3, t3_cols);
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
            let d2 = right_branch.output_columns_in_order(&expected_ctx)?[0];
            left_branch.logical_join(
                right_branch,
                null_safe_eq(column_ref(t2), column_ref(d2)),
                JoinType::Inner,
            )
        };

        let d1 = right_branch.output_columns_in_order(&expected_ctx)?[2];
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
fn test_nested_dep_join_intermediate_select_preserves_outer_ref() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let t3_cols = make_cols(&input_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let t3 = t3_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let nested_dep = {
                let left_branch = mock_scan_with_columns(2, t2_cols.clone());
                let right_branch = mock_scan_with_columns(3, t3_cols)
                    .logical_select(column_ref(t3).eq(column_ref(t2)));
                left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
            };
            nested_dep.logical_select(column_ref(t2).gt(column_ref(t1)))
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t3_cols = make_cols(&expected_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let t3 = t3_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone());
        let right_branch = {
            let (domain, domain_outputs) = create_domain_with_aliases(
                &expected_ctx,
                mock_scan_with_columns(1, t1_cols),
                vec![t1],
            )?;
            let d1 = domain_outputs[0];
            let nested_join = {
                let left_branch = mock_scan_with_columns(2, t2_cols);
                let right_branch = mock_scan_with_columns(3, t3_cols)
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

        let d1 = right_branch.output_columns_in_order(&expected_ctx)?[0];
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
/// |--- LeftOuter Join [d1 IS NOT DISTINCT FROM T2.v2]
///      |--- Domain(T1.v1) -> d1
///      |--- Aggregate [keys: T2.v2 := T2.v2]
///           |--- Select [T2.v2 = T2.v2]
///                |--- Scan(T2)
#[test]
fn test_complex_operators() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let agg_input = mock_scan_with_columns(2, t2_cols)
                .logical_select(column_ref(t2).eq(column_ref(t1)));
            aggregate_with_outputs(
                &input_ctx,
                agg_input,
                std::iter::empty(),
                std::iter::empty(),
            )?
            .0
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone());
        let right_branch = {
            let (domain, domain_outputs) = create_domain_with_aliases(
                &expected_ctx,
                mock_scan_with_columns(1, t1_cols),
                vec![t1],
            )?;
            let d1 = domain_outputs[0];
            let agg_input = mock_scan_with_columns(2, t2_cols)
                .logical_select(column_ref(t2).eq(column_ref(t2)));
            let (agg, agg_keys, _) = aggregate_with_outputs(
                &expected_ctx,
                agg_input,
                std::iter::empty(),
                [column_ref(t2)],
            )?;
            domain.logical_join(
                agg,
                null_safe_eq(column_ref(d1), column_ref(agg_keys[0])),
                JoinType::LeftOuter,
            )
        };

        let d1 = right_branch.output_columns_in_order(&expected_ctx)?[0];
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
fn test_join_equivalence_merging() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let t3_cols = make_cols(&input_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let t3 = t3_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let left_sel = mock_scan_with_columns(2, t2_cols)
                .logical_select(column_ref(t2).eq(column_ref(t1)));
            let right_sel = mock_scan_with_columns(3, t3_cols)
                .logical_select(column_ref(t3).eq(column_ref(t1)));
            left_sel.logical_join(right_sel, boolean(true), JoinType::Inner)
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t3_cols = make_cols(&expected_ctx, 1);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let t3 = t3_cols[0];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let left_sel = mock_scan_with_columns(2, t2_cols)
                .logical_select(column_ref(t2).eq(column_ref(t2)));
            let right_sel = mock_scan_with_columns(3, t3_cols)
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

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
fn test_multi_outer_refs_domain_retained() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 2);
        let t2_cols = make_cols(&input_ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t2c1 = t2_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let select_input = mock_scan_with_columns(2, t2_cols).logical_select(
                column_ref(t2c0)
                    .gt(column_ref(t1c0))
                    .and(column_ref(t2c1).gt(column_ref(t1c1))),
            );
            aggregate_with_outputs(
                &input_ctx,
                select_input,
                std::iter::empty(),
                [column_ref(t2c0)],
            )?
            .0
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 2);
        let t2_cols = make_cols(&expected_ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t2c1 = t2_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone());
        let right_branch = {
            let (domain, domain_outputs) = create_domain_with_aliases(
                &expected_ctx,
                mock_scan_with_columns(1, t1_cols),
                vec![t1c0, t1c1],
            )?;
            let d1 = domain_outputs[0];
            let d2 = domain_outputs[1];
            let join_input = domain.logical_join(
                mock_scan_with_columns(2, t2_cols),
                column_ref(t2c0)
                    .gt(column_ref(d1))
                    .and(column_ref(t2c1).gt(column_ref(d2))),
                JoinType::Inner,
            );
            let (agg, agg_keys, _) = aggregate_with_outputs(
                &expected_ctx,
                join_input,
                std::iter::empty(),
                [column_ref(t2c0), column_ref(d1), column_ref(d2)],
            )?;
            let _ = agg_keys;
            agg
        };

        let agg_keys = right_branch.output_columns_in_order(&expected_ctx)?[..3].to_vec();
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(agg_keys[1]))
                .and(null_safe_eq(column_ref(t1c1), column_ref(agg_keys[2]))),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
fn test_deep_nesting_multiple_dependencies() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 2);
        let t2_cols = make_cols(&input_ctx, 1);
        let t3_cols = make_cols(&input_ctx, 1);
        let t4_cols = make_cols(&input_ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];
        let t4c0 = t4_cols[0];
        let t4c1 = t4_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols);
            let mid_right = {
                let inner_left = mock_scan_with_columns(3, t3_cols);
                let inner_right = mock_scan_with_columns(4, t4_cols).logical_select(
                    column_ref(t4c0)
                        .eq(column_ref(t3c0))
                        .and(column_ref(t4c0).eq(column_ref(t2c0)))
                        .and(column_ref(t4c0).eq(column_ref(t1c0)))
                        .and(column_ref(t4c1).eq(column_ref(t1c1))),
                );
                inner_left.logical_dependent_join(inner_right, boolean(true), JoinType::Inner)
            };
            mid_left.logical_dependent_join(mid_right, boolean(true), JoinType::Inner)
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 2);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t3_cols = make_cols(&expected_ctx, 1);
        let t4_cols = make_cols(&expected_ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];
        let t4c0 = t4_cols[0];
        let t4c1 = t4_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols);
            let mid_right = {
                let inner_left = mock_scan_with_columns(3, t3_cols);
                let inner_right = mock_scan_with_columns(4, t4_cols).logical_select(
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

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
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
///           |--- Project [T4.v4, T4.v5]
///                |--- Select [T4.v4 = T2.v1]
///                     |--- Scan(T4) [v4, v5]
///
/// OUTPUT:
/// Join [T1.v0 IS NOT DISTINCT FROM d1]
/// |--- Scan(T1) [v0]
/// |--- Join [T2.v1 IS NOT DISTINCT FROM d2]
///      |--- Scan(T2) [v1]
///      |--- Join [T3.v3 = T4.v5 && d2 IS NOT DISTINCT FROM T4.v4]
///           |--- OrderBy [T3.v2 ASC]
///           |    |--- Project [T3.v2, T3.v3, d1, d2]
///           |         |--- Join [T3.v2 > d2 && T3.v3 > d1]
///           |              |--- Join [true]
///           |              |    |--- Domain(T2.v1) -> d2
///           |              |    |--- Domain(T1.v0) -> d1
///           |              |--- Scan(T3) [v2, v3]
///           |--- Project [T4.v4, T4.v5]
///                |--- Select [T4.v4 = T4.v4]
///                     |--- Scan(T4) [v4, v5]
#[test]
fn test_nested_dep_join_regular_join_orderby_project_domain_vs_repr() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let t3_cols = make_cols(&input_ctx, 2);
        let t4_cols = make_cols(&input_ctx, 2);
        let t1c0 = t1_cols[0];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];
        let t3c1 = t3_cols[1];
        let t4c0 = t4_cols[0];
        let t4c1 = t4_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let mid_left = mock_scan_with_columns(2, t2_cols);
            let mid_right = {
                let left_regular = {
                    let select_input = mock_scan_with_columns(3, t3_cols).logical_select(
                        column_ref(t3c0)
                            .gt(column_ref(t2c0))
                            .and(column_ref(t3c1).gt(column_ref(t1c0))),
                    );
                    let (projected, projected_cols) = project_with_outputs(
                        &input_ctx,
                        select_input,
                        [column_ref(t3c0), column_ref(t3c1)],
                    )?;
                    OrderBy::new(
                        projected,
                        vec![(column_ref(projected_cols[0]), TupleOrderingDirection::Asc)],
                    )
                    .into_operator()
                };
                let left_outputs = left_regular.output_columns_in_order(&input_ctx)?;
                let p3b = left_outputs[1];
                let right_regular = {
                    let select_input = mock_scan_with_columns(4, t4_cols)
                        .logical_select(column_ref(t4c0).eq(column_ref(t2c0)));
                    let (projected, projected_cols) = project_with_outputs(
                        &input_ctx,
                        select_input,
                        [column_ref(t4c0), column_ref(t4c1)],
                    )?;
                    let p4b = projected_cols[1];
                    (projected, p4b)
                };
                left_regular.logical_join(
                    right_regular.0,
                    column_ref(p3b).eq(column_ref(right_regular.1)),
                    JoinType::Inner,
                )
            };
            mid_left.logical_dependent_join(mid_right, boolean(true), JoinType::Inner)
        };

        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t3_cols = make_cols(&expected_ctx, 2);
        let t4_cols = make_cols(&expected_ctx, 2);
        let t1c0 = t1_cols[0];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];
        let t3c1 = t3_cols[1];
        let t4c0 = t4_cols[0];
        let t4c1 = t4_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone());
        let (_input_left_project, input_left_outputs) = project_with_outputs(
            &expected_ctx,
            mock_scan_with_columns(3, t3_cols.clone()).logical_select(
                column_ref(t3c0)
                    .gt(column_ref(t2c0))
                    .and(column_ref(t3c1).gt(column_ref(t1c0))),
            ),
            [column_ref(t3c0), column_ref(t3c1)],
        )?;
        let p3a = input_left_outputs[0];
        let p3b = input_left_outputs[1];
        let (_input_right_project, input_right_outputs) = project_with_outputs(
            &expected_ctx,
            mock_scan_with_columns(4, t4_cols.clone())
                .logical_select(column_ref(t4c0).eq(column_ref(t2c0))),
            [column_ref(t4c0), column_ref(t4c1)],
        )?;
        let p4b = input_right_outputs[1];
        let (right_branch, d1) = {
            let mid_left = mock_scan_with_columns(2, t2_cols.clone());
            let (mid_right, d1) = {
                let left_regular = {
                    let (current_domain, current_outputs) = create_domain_with_aliases(
                        &expected_ctx,
                        mock_scan_with_columns(2, t2_cols),
                        vec![t2c0],
                    )?;
                    let d2 = current_outputs[0];
                    let (parent_domain, parent_outputs) = create_domain_with_aliases(
                        &expected_ctx,
                        mock_scan_with_columns(1, t1_cols),
                        vec![t1c0],
                    )?;
                    let d1 = parent_outputs[0];
                    let select_input = current_domain
                        .logical_join(parent_domain, boolean(true), JoinType::Inner)
                        .logical_join(
                            mock_scan_with_columns(3, t3_cols),
                            column_ref(t3c0)
                                .gt(column_ref(d2))
                                .and(column_ref(t3c1).gt(column_ref(d1))),
                            JoinType::Inner,
                        );
                    let (projected, projected_cols) = project_with_outputs(
                        &expected_ctx,
                        select_input,
                        [
                            column_ref(t3c0),
                            column_ref(t3c1),
                            column_ref(d1),
                            column_ref(d2),
                        ],
                    )?;
                    let ordered = OrderBy::new(
                        projected,
                        vec![(column_ref(p3a), TupleOrderingDirection::Asc)],
                    )
                    .into_operator();
                    (ordered, projected_cols, d1)
                };
                let (left_regular, left_outputs, d1) = left_regular;
                let d2 = left_outputs[3];
                let (right_regular, right_outputs) = {
                    let select_input = mock_scan_with_columns(4, t4_cols)
                        .logical_select(column_ref(t4c0).eq(column_ref(t4c0)));
                    let (projected, projected_cols) = project_with_outputs(
                        &expected_ctx,
                        select_input,
                        [column_ref(t4c0), column_ref(t4c1)],
                    )?;
                    (projected, projected_cols)
                };
                let p4a = right_outputs[0];
                let inner_join = left_regular.logical_join(
                    right_regular,
                    column_ref(p3b)
                        .eq(column_ref(p4b))
                        .and(null_safe_eq(column_ref(d2), column_ref(p4a))),
                    JoinType::Inner,
                );
                (
                    mid_left.logical_join(
                        inner_join,
                        null_safe_eq(column_ref(t2c0), column_ref(d2)),
                        JoinType::Inner,
                    ),
                    d1,
                )
            };
            (mid_right, d1)
        };

        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(d1)),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
}

/// Test Left Join Right-Side Equivalence Is Not Used As Outer Representative:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0]
/// |--- Join [true] (LeftOuter)
///      |--- Scan(T2) [v1]
///      |--- Select [T3.v2 = T1.v0]
///           |--- Scan(T3) [v2]
///
/// EXPECTATION:
/// The top-level decorrelated join condition must not use T3.v2 as the
/// representative for T1.v0, because T3.v2 is on the non-preserved side of
/// a left join and can be NULL-extended.
#[test]
fn test_left_join_right_cclass_not_propagated() -> Result<()> {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t3_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];
    let t3 = t3_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let left_regular = mock_scan_with_columns(2, t2_cols);
            let right_regular = mock_scan_with_columns(3, t3_cols)
                .logical_select(column_ref(t3).eq(column_ref(t1)));
            left_regular.logical_join(right_regular, boolean(true), JoinType::LeftOuter)
        };
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let res = UnnestingRule::new().apply(input_plan, &ctx)?;

    let root_join = match &res.kind {
        OperatorKind::Join(meta) => Join::borrow_raw_parts(meta, &res.common),
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
    Ok(())
}

/// Test Nested Left-Preserving Dependent Join Does Not Propagate Inner Representative:
///
/// INPUT:
/// DependentJoin
/// |--- Scan(T1) [v0]
/// |--- DependentJoin (LeftSemi or LeftAnti)
///      |--- Scan(T2) [v1]
///      |--- Select [T3.v2 = T1.v0]
///           |--- Scan(T3) [v2]
///
/// EXPECTATION:
/// The top-level decorrelated join condition must not use T3.v2 as the
/// representative for T1.v0, because left-semi and left-anti joins do not
/// expose right-side columns in their output.
fn assert_nested_left_preserving_dep_join_keeps_domain_repr(
    nested_join_type: JoinType,
) -> Result<()> {
    let ctx = IRContext::with_empty_magic();
    let t1_cols = make_cols(&ctx, 1);
    let t2_cols = make_cols(&ctx, 1);
    let t3_cols = make_cols(&ctx, 1);
    let t1 = t1_cols[0];
    let t2 = t2_cols[0];
    let t3 = t3_cols[0];

    let input_plan = {
        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = {
            let nested_outer = mock_scan_with_columns(2, t2_cols);
            let nested_inner = mock_scan_with_columns(3, t3_cols)
                .logical_select(column_ref(t3).eq(column_ref(t1)));
            nested_outer.logical_dependent_join(nested_inner, boolean(true), nested_join_type)
        };
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let res = UnnestingRule::new().apply(input_plan, &ctx)?;

    let root_join = match &res.kind {
        OperatorKind::Join(meta) => Join::borrow_raw_parts(meta, &res.common),
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
        "top join condition must not use inner-only semi/anti column as outer repr; plan:\n{}",
        quick_explain(&res, &ctx)
    );
    assert!(
        used_cols.iter().any(|c| *c != t1 && *c != t2 && *c != t3),
        "expected a domain representative column in top join condition; plan:\n{}",
        quick_explain(&res, &ctx)
    );
    assert!(
        !root_join.inner().output_columns(&ctx)?.contains(&t3),
        "nested semi/anti join output must not expose the inner scan column; plan:\n{}",
        quick_explain(&res, &ctx)
    );
    Ok(())
}

/// Test Nested Left-Semi Join Keeps Domain Representative
#[test]
fn test_nested_left_semi_inner_repr_not_propagated() -> Result<()> {
    assert_nested_left_preserving_dep_join_keeps_domain_repr(JoinType::LeftSemi)
}

/// Test Nested Left-Anti Join Keeps Domain Representative
#[test]
fn test_nested_left_anti_inner_repr_not_propagated() -> Result<()> {
    assert_nested_left_preserving_dep_join_keeps_domain_repr(JoinType::LeftAnti)
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
fn test_partial_repr_resolution_all_or_nothing() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 2);
        let t2_cols = make_cols(&input_ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t2c1 = t2_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch = mock_scan_with_columns(2, t2_cols).logical_select(
            column_ref(t2c0)
                .eq(column_ref(t1c0))
                .and(column_ref(t2c1).gt(column_ref(t1c1))),
        );
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Inner)
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 2);
        let t2_cols = make_cols(&expected_ctx, 2);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t2c1 = t2_cols[1];

        let left_branch = mock_scan_with_columns(1, t1_cols.clone());
        let right_branch = {
            let (domain, domain_outputs) = create_domain_with_aliases(
                &expected_ctx,
                mock_scan_with_columns(1, t1_cols),
                vec![t1c0, t1c1],
            )?;
            let d1 = domain_outputs[0];
            let d2 = domain_outputs[1];
            domain.logical_join(
                mock_scan_with_columns(2, t2_cols),
                column_ref(t2c0)
                    .eq(column_ref(d1))
                    .and(column_ref(t2c1).gt(column_ref(d2))),
                JoinType::Inner,
            )
        };

        let domain_outputs = right_branch.output_columns_in_order(&expected_ctx)?[..2].to_vec();
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1c0), column_ref(domain_outputs[0])).and(null_safe_eq(
                column_ref(t1c1),
                column_ref(domain_outputs[1]),
            )),
            JoinType::Inner,
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
}

/// Test Simple Mark Unnesting:
///
/// INPUT:
/// DependentJoin(Mark)
/// |--- Scan(T1)
/// |--- Select [T2.v2 = T1.v1]
///      |--- Scan(T2)
///
/// OUTPUT:
/// Join(Mark) [T1.v1 IS NOT DISTINCT FROM T2.v2]
/// |--- Scan(T1)
/// |--- Select [T2.v2 = T2.v2]
///      |--- Scan(T2)
#[test]
fn test_simple_mark_unnesting() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 1);
        let t2_cols = make_cols(&input_ctx, 1);
        let mark = make_mark_col(&input_ctx);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch =
            mock_scan_with_columns(2, t2_cols).logical_select(column_ref(t2).eq(column_ref(t1)));
        left_branch.logical_dependent_join(right_branch, boolean(true), JoinType::Mark(mark))
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 1);
        let t2_cols = make_cols(&expected_ctx, 1);
        let mark = make_mark_col(&expected_ctx);
        let t1 = t1_cols[0];
        let t2 = t2_cols[0];
        let left_branch = mock_scan_with_columns(1, t1_cols);
        let right_branch =
            mock_scan_with_columns(2, t2_cols).logical_select(column_ref(t2).eq(column_ref(t2)));
        left_branch.logical_join(
            right_branch,
            null_safe_eq(column_ref(t1), column_ref(t2)),
            JoinType::Mark(mark),
        )
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
}

/// Test Two Conjunctive Mark Joins Feed One Boolean Filter:
/// 
/// This models a shape produced from SQL like:
///
/// SELECT * FROM T1
/// WHERE T1.v0 IN (SELECT T2.v2 FROM T2)
///    OR T1.v1 IN (SELECT T3.v3 FROM T3);
///
/// Or like:
///
/// SELECT * FROM T1
/// WHERE EXISTS (SELECT 1 FROM T2 WHERE T2.v2 = T1.v0)
///    OR EXISTS (SELECT 1 FROM T3 WHERE T3.v3 = T1.v1);
///
/// INPUT:
/// Select [m1 OR m2]
/// |--- DependentJoin(Mark(m2))
///      |--- DependentJoin(Mark(m1))
///      |    |--- Scan(T1) [v0, v1]
///      |    |--- Select [T2.v2 = T1.v0]
///      |         |--- Scan(T2) [v2]
///      |--- Select [T3.v3 = T1.v1]
///           |--- Scan(T3) [v3]
///
/// OUTPUT:
/// Select [m1 OR m2]
/// |--- Join(Mark(m2)) [T1.v1 IS NOT DISTINCT FROM T3.v3]
///      |--- Join(Mark(m1)) [T1.v0 IS NOT DISTINCT FROM T2.v2]
///      |    |--- Scan(T1) [v0, v1]
///      |    |--- Select [T2.v2 = T2.v2]
///      |    |    |--- Scan(T2) [v2]
///      |--- Select [T3.v3 = T3.v3]
///           |--- Scan(T3) [v3]
#[test]
fn test_two_mark_joins_under_or_filter() -> Result<()> {
    let input_ctx = IRContext::with_empty_magic();
    let expected_ctx = IRContext::with_empty_magic();

    let input_plan = {
        let t1_cols = make_cols(&input_ctx, 2);
        let t2_cols = make_cols(&input_ctx, 1);
        let t3_cols = make_cols(&input_ctx, 1);
        let mark1 = make_mark_col(&input_ctx);
        let mark2 = make_mark_col(&input_ctx);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];

        let first_mark = mock_scan_with_columns(1, t1_cols).logical_dependent_join(
            mock_scan_with_columns(2, t2_cols).logical_select(column_ref(t2c0).eq(column_ref(t1c0))),
            boolean(true),
            JoinType::Mark(mark1),
        );
        first_mark
            .logical_dependent_join(
                mock_scan_with_columns(3, t3_cols)
                    .logical_select(column_ref(t3c0).eq(column_ref(t1c1))),
                boolean(true),
                JoinType::Mark(mark2),
            )
            .logical_select(column_ref(mark1).or(column_ref(mark2)))
    };

    let expected_plan = {
        let t1_cols = make_cols(&expected_ctx, 2);
        let t2_cols = make_cols(&expected_ctx, 1);
        let t3_cols = make_cols(&expected_ctx, 1);
        let mark1 = make_mark_col(&expected_ctx);
        let mark2 = make_mark_col(&expected_ctx);
        let t1c0 = t1_cols[0];
        let t1c1 = t1_cols[1];
        let t2c0 = t2_cols[0];
        let t3c0 = t3_cols[0];

        mock_scan_with_columns(1, t1_cols)
            .logical_join(
                mock_scan_with_columns(2, t2_cols).logical_select(column_ref(t2c0).eq(column_ref(t2c0))),
                null_safe_eq(column_ref(t1c0), column_ref(t2c0)),
                JoinType::Mark(mark1),
            )
            .logical_join(
                mock_scan_with_columns(3, t3_cols).logical_select(column_ref(t3c0).eq(column_ref(t3c0))),
                null_safe_eq(column_ref(t1c1), column_ref(t3c0)),
                JoinType::Mark(mark2),
            )
            .logical_select(column_ref(mark1).or(column_ref(mark2)))
    };

    assert_unnesting(&input_ctx, input_plan, &expected_ctx, expected_plan)
}
