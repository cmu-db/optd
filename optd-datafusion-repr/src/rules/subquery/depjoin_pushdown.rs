// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::{PlanNodeOrGroup, PredNode};
// TODO: No push past join
// TODO: Sideways information passing??
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, BinOpPred, BinOpType, ColumnRefPred, ConstantPred, DependentJoin,
    DfNodeType, DfPredType, DfReprPlanNode, DfReprPredNode, ExternColumnRefPred, JoinType,
    ListPred, LogOpPred, LogOpType, LogicalAgg, LogicalFilter, LogicalJoin, LogicalProjection,
    PredExt, RawDependentJoin,
};
use crate::rules::macros::define_rule;
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

define_rule!(
    DepInitialDistinct,
    apply_dep_initial_distinct,
    (RawDepJoin(JoinType::Cross), left, right)
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

    // If we have no correlated columns, just emit a cross join instead
    if correlated_col_indices.is_empty() {
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

    let new_dep_join = DependentJoin::new_unchecked(
        distinct_agg_node.into_plan_node(),
        right,
        cond,
        extern_cols,
        JoinType::Cross,
    );

    // Our join condition is going to make sure that all of the correlated columns
    // in the right side are equal to their equivalent columns in the left side.
    // (they will have the same index, just shifted over)
    let join_cond = LogOpPred::new(
        LogOpType::And,
        (0..correlated_col_indices.len())
            .map(|i| {
                assert!(i + left_schema_size < left_schema_size + right_schema_size);
                BinOpPred::new(
                    ColumnRefPred::new(i).into_pred_node(),
                    ColumnRefPred::new(i + left_schema_size).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node()
            })
            .collect(),
    );

    let new_join = LogicalJoin::new_unchecked(
        left,
        new_dep_join.into_plan_node(),
        join_cond.into_pred_node(),
        JoinType::Inner,
    );

    // Ensure that the schema above the new_join is the same as it was before
    // for correctness (Project the left side of the new join,
    // plus the *right side of the right side*)
    let new_proj = LogicalProjection::new(
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
    );

    vec![new_proj.into_plan_node().into()]
}

define_rule!(
    DepJoinPastProj,
    apply_dep_join_past_proj,
    (DepJoin(JoinType::Cross), left, (Projection, right))
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
    let right = proj.child();

    // TODO: can we have external columns in projection node? I don't think so?
    // Cross join should always have true cond
    assert!(cond == ConstantPred::bool(true).into_pred_node());
    let left_schema_len = optimizer.get_schema_of(left.clone()).len();
    let right_schema_len = optimizer.get_schema_of(right.clone()).len();

    let right_cols_proj =
        (0..right_schema_len).map(|x| ColumnRefPred::new(x + left_schema_len).into_pred_node());

    let left_cols_proj = (0..left_schema_len).map(|x| ColumnRefPred::new(x).into_pred_node());
    let new_proj_exprs = ListPred::new(
        left_cols_proj
            .chain(right_cols_proj)
            .map(|x| x.into_pred_node())
            .collect(),
    );

    let new_dep_join =
        DependentJoin::new_unchecked(left, right, cond, extern_cols, JoinType::Cross);
    let new_proj = LogicalProjection::new(new_dep_join.into_plan_node(), new_proj_exprs);

    vec![new_proj.into_plan_node().into()]
}

define_rule!(
    DepJoinPastFilter,
    apply_dep_join_past_filter,
    (DepJoin(JoinType::Cross), left, (Filter, right))
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
        JoinType::Cross,
    );

    let new_filter = LogicalFilter::new(new_dep_join.into_plan_node(), rewritten_expr);

    vec![new_filter.into_plan_node().into()]
}

define_rule!(
    DepJoinPastAgg,
    apply_dep_join_past_agg,
    (DepJoin(JoinType::Cross), left, (Agg, right))
);

/// Pushes a dependent join past an aggregation node
/// We need to append the correlated columns into the aggregation node,
/// and add a left outer join with the left side of the dependent join (the
/// deduplicated set).
/// For info on why we do the outer join, refer to the Unnesting Arbitrary Queries
/// talk by Mark Raasveldt. The correlated columns are covered in the original paper.
///
/// TODO: the outer join is not implemented yet, so some edge cases won't work.
///       Run SQList tests to catch these, I guess.
fn apply_dep_join_past_agg(
    _optimizer: &impl Optimizer<DfNodeType>,
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

    let new_groups = ListPred::new(
        groups
            .to_vec()
            .into_iter()
            .map(|x| {
                x.rewrite_column_refs(|col| Some(col + correlated_col_indices.len()))
                    .unwrap()
            })
            .chain(correlated_col_indices.iter().map(|x| {
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

    let new_dep_join =
        DependentJoin::new_unchecked(left, right, cond, extern_cols, JoinType::Cross);

    let new_agg = LogicalAgg::new(new_dep_join.into_plan_node(), new_exprs, new_groups);

    vec![new_agg.into_plan_node().into()]
}

// Heuristics-only rule. If we don't have references to the external columns on the right side,
// we can rewrite the dependent join into a normal join.
define_rule!(
    DepJoinEliminate,
    apply_dep_join_eliminate_at_scan, // TODO matching is all wrong
    (DepJoin(JoinType::Cross), left, right)
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
