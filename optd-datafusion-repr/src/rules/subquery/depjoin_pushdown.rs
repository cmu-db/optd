// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::{PlanNodeOrGroup, PredNode};
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, BinOpPred, BinOpType, ColumnRefPred, ConstantPred, DependentJoin,
    DfNodeType, DfPredType, DfReprPlanNode, DfReprPredNode, ExternColumnRefPred, FuncPred,
    FuncType, JoinType, ListPred, LogOpPred, LogOpType, LogicalAgg, LogicalFilter, LogicalJoin,
    LogicalLimit, LogicalProjection, PredExt, RawDependentJoin, SubqueryType,
};
use crate::rules::macros::{define_rule, define_rule_discriminant};
use crate::OptimizerExt;

/// Like rewrite_column_refs, except it translates ExternColumnRefs into ColumnRefs
fn rewrite_extern_column_refs(
    expr: ArcDfPredNode,
    rewrite_fn: &mut impl FnMut(usize) -> Option<usize>,
) -> Option<ArcDfPredNode> {
    if let Some(col_ref) = ExternColumnRefPred::from_pred_node(expr.clone()) {
        let rewritten = rewrite_fn(col_ref.index());
        return if let Some(rewritten_idx) = rewritten {
            let new_col_ref = ColumnRefPred::new(rewritten_idx);
            Some(new_col_ref.into_pred_node())
        } else {
            None
        };
    }

    let children = expr
        .children
        .clone()
        .into_iter()
        .map(|child| rewrite_extern_column_refs(child, rewrite_fn))
        .collect::<Option<Vec<_>>>()?;
    Some(
        PredNode {
            typ: expr.typ.clone(),
            children,
            data: expr.data.clone(),
        }
        .into(),
    )
}

define_rule_discriminant!(
    DepInitialDistinct,
    apply_dep_initial_distinct,
    (RawDepJoin(SubqueryType::Scalar), left, right)
);

/// Initial rule to generate a join above this dependent join, and push the dependent
/// join further into the right side.
/// This is valuable because the left side of the dependent join will be a set, with only distinct
/// values. This makes later transformations valid, and has to happen first.
/// More information can be found in the "Unnesting Arbitrary Queries" paper.
fn apply_dep_initial_distinct(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = RawDependentJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let cond = join.cond();
    let extern_cols = join.extern_cols();

    assert!(cond == ConstantPred::bool(true).into_pred_node());

    let left_schema_size = optimizer.get_schema_of(left.clone()).len();
    let right_schema_size = optimizer.get_schema_of(right.clone()).len();

    let correlated_col_indices = extern_cols
        .to_vec()
        .into_iter()
        .map(|x| ExternColumnRefPred::from_pred_node(x).unwrap().index())
        .collect::<Vec<usize>>();

    // If we have no correlated columns, we can skip the whole dependent join step
    if correlated_col_indices.is_empty() {
        let res = match join.sq_type() {
            SubqueryType::Scalar => LogicalJoin::new_unchecked(
                left,
                right,
                ConstantPred::bool(true).into_pred_node(),
                JoinType::Cross,
            )
            .into_plan_node(),
            SubqueryType::Exists => {
                let right_lim_1 = LogicalLimit::new_unchecked(
                    right,
                    ConstantPred::int64(0).into_pred_node(),
                    ConstantPred::int64(1).into_pred_node(),
                )
                .into_plan_node();
                let right_count_star = LogicalAgg::new(
                    right_lim_1,
                    ListPred::new(vec![FuncPred::new(
                        FuncType::Agg("count".to_string()),
                        ListPred::new(vec![ConstantPred::int64(1).into_pred_node()]),
                    )
                    .into_pred_node()]),
                    ListPred::new(vec![]),
                )
                .into_plan_node();

                let count_star_to_bool_proj = LogicalProjection::new(
                    right_count_star,
                    ListPred::new(vec![BinOpPred::new(
                        ColumnRefPred::new(0).into_pred_node(),
                        ConstantPred::int64(0).into_pred_node(),
                        BinOpType::Gt,
                    )
                    .into_pred_node()]),
                )
                .into_plan_node();

                LogicalJoin::new_unchecked(
                    left,
                    count_star_to_bool_proj,
                    ConstantPred::bool(true).into_pred_node(),
                    JoinType::Cross,
                )
                .into_plan_node()
            }
            SubqueryType::Any { pred, op } => LogicalJoin::new_unchecked(
                left,
                right,
                BinOpPred::new(
                    pred.clone().into(),
                    ColumnRefPred::new(left_schema_size).into_pred_node(),
                    *op,
                )
                .into_pred_node(),
                JoinType::LeftMark,
            )
            .into_plan_node(),
        };

        return vec![res.into()];
    }
    if correlated_col_indices.is_empty() && matches!(join.sq_type(), SubqueryType::Scalar) {
        let new_join = LogicalJoin::new_unchecked(
            left,
            right,
            ConstantPred::bool(true).into_pred_node(),
            JoinType::Cross,
        );

        return vec![new_join.into_plan_node().into()];
    }

    // An aggregate node that groups by all correlated columns allows us to
    // effectively get the domain
    let distinct_agg_node = LogicalAgg::new_unchecked(
        left.clone(),
        ListPred::new(vec![]),
        ListPred::new(
            correlated_col_indices
                .iter()
                .map(|x| ColumnRefPred::new(*x).into_pred_node())
                .collect(),
        ),
    );

    let new_dep_join_schema_size = correlated_col_indices.len() + right_schema_size;
    let new_dep_join =
        DependentJoin::new_unchecked(distinct_agg_node.into_plan_node(), right, cond, extern_cols);

    // Our join condition is going to make sure that all of the correlated columns
    // in the right side are equal to their equivalent columns in the left side.
    //
    // If we have correlated columns [#16, #17], we want our condition to be:
    // #16 = #0 AND #17 = #1
    //
    // This is because the aggregate we install on the right side will map the
    // correlated columns to their respective indices as shown.
    debug_assert!(!correlated_col_indices.is_empty());
    let join_cond = match join.sq_type() {
        SubqueryType::Scalar | SubqueryType::Exists => LogOpPred::new(
            LogOpType::And,
            correlated_col_indices
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    assert!(i + left_schema_size < left_schema_size + new_dep_join_schema_size);
                    BinOpPred::new(
                        ColumnRefPred::new(*x).into_pred_node(),
                        ColumnRefPred::new(i + left_schema_size).into_pred_node(),
                        BinOpType::Eq,
                    )
                    .into_pred_node()
                })
                .collect(),
        ),
        SubqueryType::Any { pred, op } => LogOpPred::new(
            LogOpType::And,
            correlated_col_indices
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    assert!(i + left_schema_size < left_schema_size + new_dep_join_schema_size);
                    BinOpPred::new(
                        pred.clone().into(),
                        ColumnRefPred::new(i + left_schema_size).into_pred_node(),
                        *op,
                    )
                    .into_pred_node()
                })
                .collect(),
        ),
    };

    let join_type = match join.sq_type() {
        SubqueryType::Scalar => JoinType::Inner,
        SubqueryType::Exists | SubqueryType::Any { pred: _, op: _ } => JoinType::LeftMark,
    };

    let new_join = LogicalJoin::new_unchecked(
        left,
        new_dep_join.into_plan_node(),
        join_cond.into_pred_node(),
        join_type,
    );

    // Ensure that the schema above the new_join is the same as it was before
    // for correctness (Project the left side of the new join,
    // plus the *right side of the right side*)
    let node = if matches!(join.sq_type(), SubqueryType::Scalar) {
        LogicalProjection::new(
            new_join.into_plan_node(),
            ListPred::new(
                (0..left_schema_size)
                    .chain(
                        (left_schema_size + correlated_col_indices.len())
                            ..(left_schema_size + correlated_col_indices.len() + right_schema_size),
                    )
                    .map(|x| ColumnRefPred::new(x).into_pred_node())
                    .collect(),
            ),
        )
        .into_plan_node()
    } else {
        new_join.into_plan_node()
    };

    vec![node.into()]
}

define_rule!(
    DepJoinPastProj,
    apply_dep_join_past_proj,
    (DepJoin, left, (Projection, right))
);

/// Pushes a dependent join past a projection node.
/// The new projection node above the dependent join is changed to include the columns
/// from both sides of the dependent join. Otherwise, this transformation is trivial.
fn apply_dep_join_past_proj(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = DependentJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let cond = join.cond();
    let extern_cols = join.extern_cols();
    let proj = LogicalProjection::from_plan_node(right.unwrap_plan_node()).unwrap();
    let proj_exprs = proj.exprs();
    let right = proj.child();

    // TODO: can we have external columns in projection node? I don't think so?
    // Cross join should always have true cond
    assert!(cond == ConstantPred::bool(true).into_pred_node());
    let left_schema_len = optimizer.get_schema_of(left.clone()).len();

    let right_cols_proj = proj_exprs.to_vec().into_iter().map(|x| {
        x.rewrite_column_refs(|col| Some(col + left_schema_len))
            .unwrap()
    });

    let left_cols_proj = (0..left_schema_len).map(|x| ColumnRefPred::new(x).into_pred_node());
    let new_proj_exprs = ListPred::new(
        left_cols_proj
            .chain(right_cols_proj)
            .map(|x| x.into_pred_node())
            .collect(),
    );

    let new_dep_join = DependentJoin::new_unchecked(left, right, cond, extern_cols);
    let new_proj = LogicalProjection::new(new_dep_join.into_plan_node(), new_proj_exprs);

    vec![new_proj.into_plan_node().into()]
}

define_rule!(
    DepJoinPastFilter,
    apply_dep_join_past_filter,
    (DepJoin, left, (Filter, right))
);

/// Pushes a dependent join past a projection node.
/// The new projection node above the dependent join is changed to include the columns
/// from both sides of the dependent join. Otherwise, this transformation is trivial.
fn apply_dep_join_past_filter(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = DependentJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let cond = join.cond();
    let extern_cols = join.extern_cols();
    let filter = LogicalFilter::from_plan_node(right.unwrap_plan_node()).unwrap();
    let right = filter.child();
    let filter_cond = filter.cond();

    // Cross join should always have true cond
    assert!(cond == ConstantPred::bool(true).into_pred_node());

    let left_schema_len = optimizer.get_schema_of(left.clone()).len();

    let correlated_col_indices = extern_cols
        .to_vec()
        .into_iter()
        .map(|x| ExternColumnRefPred::from_pred_node(x).unwrap().index())
        .collect::<Vec<usize>>();

    let rewritten_expr = filter_cond
        .rewrite_column_refs(&mut |col| Some(col + left_schema_len))
        .unwrap();

    let rewritten_expr = rewrite_extern_column_refs(rewritten_expr, &mut |col| {
        let idx = correlated_col_indices
            .iter()
            .position(|&x| x == col)
            .unwrap();
        Some(idx)
    })
    .unwrap();

    let new_dep_join = DependentJoin::new_unchecked(
        left,
        right,
        cond,
        ListPred::new(
            correlated_col_indices
                .into_iter()
                .map(|x| ExternColumnRefPred::new(x).into_pred_node())
                .collect(),
        ),
    );

    let new_filter = LogicalFilter::new(new_dep_join.into_plan_node(), rewritten_expr);

    vec![new_filter.into_plan_node().into()]
}

define_rule!(
    DepJoinPastAgg,
    apply_dep_join_past_agg,
    (DepJoin, left, (Agg, right))
);

/// Pushes a dependent join past an aggregation node
/// We need to append the correlated columns into the aggregation node,
/// and add a left outer join with the left side of the dependent join (the
/// deduplicated set).
/// For info on why we do the outer join, refer to the Unnesting Arbitrary Queries
/// talk by Mark Raasveldt. The correlated columns are covered in the original paper.
fn apply_dep_join_past_agg(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = DependentJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let cond = join.cond();
    let extern_cols = join.extern_cols();
    let agg = LogicalAgg::from_plan_node(right.unwrap_plan_node()).unwrap();
    let exprs = agg.exprs();
    let groups = agg.groups();
    let right = agg.child();

    let left_schema_size = optimizer.get_schema_of(left.clone()).len();

    // Cross join should always have true cond
    assert!(cond == ConstantPred::bool(true).into_pred_node());

    // TODO: OUTER JOIN TRANSFORMATION

    let correlated_col_indices = extern_cols
        .to_vec()
        .into_iter()
        .map(|x| {
            ColumnRefPred::new(ExternColumnRefPred::from_pred_node(x).unwrap().index())
                .into_pred_node()
        })
        .collect::<Vec<_>>();

    // We need to group by all correlated columns.
    // In our initial distinct step, we installed an agg node that groups by all correlated columns.
    // Keeping this in mind, we only need to append a sequential number for each correlated column,
    // as these will correspond to the outputs of the agg node.
    let new_groups = ListPred::new(
        (0..correlated_col_indices.len())
            .map(|x| ColumnRefPred::new(x).into_pred_node())
            .chain(groups.to_vec().into_iter().map(|x| {
                x.rewrite_column_refs(|col| Some(col + correlated_col_indices.len()))
                    .unwrap()
            }))
            .collect(),
    );

    let new_exprs = ListPred::new(
        exprs
            .to_vec()
            .into_iter()
            .map(|x| {
                x.rewrite_column_refs(|col| Some(col + correlated_col_indices.len()))
                    .unwrap()
            })
            .collect(),
    );

    let new_dep_join = DependentJoin::new_unchecked(left.clone(), right, cond, extern_cols);

    let new_agg_exprs_size = new_exprs.len();
    let new_agg_groups_size = new_groups.len();
    let new_agg_schema_size = new_agg_groups_size + new_agg_exprs_size;
    let new_agg = LogicalAgg::new(new_dep_join.into_plan_node(), new_exprs, new_groups);

    // Add left outer join above the agg node, joining the deduplicated set
    // with the new agg node.

    // Both sides will have an agg now, so we want to match the correlated
    // columns from the left with those from the right
    let outer_join_cond = LogOpPred::new(
        LogOpType::And,
        correlated_col_indices
            .iter()
            .enumerate()
            .map(|(i, _)| {
                assert!(i + left_schema_size < left_schema_size + new_agg_schema_size);
                BinOpPred::new(
                    ColumnRefPred::new(i).into_pred_node(),
                    // We *prepend* the correlated columns to the groups list,
                    // so we don't need to take into account the old
                    // group-by expressions to get the corresponding correlated
                    // column.
                    ColumnRefPred::new(left_schema_size + i).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node()
            })
            .collect(),
    );

    let new_outer_join = LogicalJoin::new_unchecked(
        left,
        new_agg.into_plan_node(),
        outer_join_cond.into_pred_node(),
        JoinType::LeftOuter,
    );

    // We have to maintain the same schema above outer join as w/o it, but we
    // also need to use the groups from the deduplicated left side, and the
    // exprs from the new agg node. If we use everything from the new agg,
    // we don't maintain nulls as desired.
    let outer_join_proj = LogicalProjection::new(
        // The meaning is to take everything from the left side, and everything
        // from the right side *that is not in the left side*. I am unsure
        // of the correctness of this project in every case.
        new_outer_join.into_plan_node(),
        ListPred::new(
            (0..left_schema_size)
                .chain(left_schema_size + left_schema_size..left_schema_size + new_agg_schema_size)
                .map(|x| {
                    // Count(*) special case: We want all NULLs to be transformed into 0s.
                    if x >= left_schema_size + new_agg_groups_size {
                        // If this node corresponds to an agg function, and
                        // it's a count(*), apply the workaround
                        let expr =
                            exprs.to_vec()[x - left_schema_size - new_agg_groups_size].clone();
                        if expr.typ == DfPredType::Func(FuncType::Agg("count".to_string())) {
                            let expr_child = expr.child(0).child(0);
                            // Any count(constant)should be treated as `count(*)`
                            if let DfPredType::Constant(constant_typ) = expr_child.typ {
                                return FuncPred::new(
                                    FuncType::Scalar(
                                        "coalesce".to_string(),
                                        constant_typ.into_data_type(),
                                    ),
                                    ListPred::new(vec![
                                        ColumnRefPred::new(x).into_pred_node(),
                                        ConstantPred::int64(0).into_pred_node(),
                                    ]),
                                )
                                .into_pred_node();
                            }
                        }
                    }

                    ColumnRefPred::new(x).into_pred_node()
                })
                .collect(),
        ),
    );

    vec![outer_join_proj.into_plan_node().into()]
}

// Heuristics-only rule. If we don't have references to the external columns on the right side,
// we can rewrite the dependent join into a normal join.
define_rule!(
    DepJoinEliminate,
    apply_dep_join_eliminate_at_scan, // TODO matching is all wrong
    (DepJoin, left, right)
);

/// If we've gone all the way down to the scan node, we can swap the dependent join
/// for an inner join! Our main mission is complete!
fn apply_dep_join_eliminate_at_scan(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = DependentJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let cond = join.cond();

    // Cross join should always have true cond
    assert!(cond == ConstantPred::bool(true).into_pred_node());

    fn inspect_pred(node: &ArcDfPredNode) -> bool {
        if node.typ == DfPredType::ExternColumnRef {
            return false;
        }
        for child in &node.children {
            if !inspect_pred(child) {
                return false;
            }
        }
        true
    }

    fn inspect_plan_node(node: &ArcDfPlanNode) -> bool {
        for child in &node.children {
            if !inspect_plan_node(&child.unwrap_plan_node()) {
                return false;
            }
        }
        for pred in &node.predicates {
            if !inspect_pred(pred) {
                return false;
            }
        }
        true
    }

    if inspect_plan_node(&right.unwrap_plan_node()) {
        let new_join = LogicalJoin::new_unchecked(
            left,
            right,
            ConstantPred::bool(true).into_pred_node(),
            JoinType::Inner,
        );
        vec![new_join.into_plan_node().into()]
    } else {
        vec![]
    }
}
