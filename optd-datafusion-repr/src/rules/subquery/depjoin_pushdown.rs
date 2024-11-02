// TODO: No push past join
// TODO: Sideways information passing??
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};
use std::collections::HashMap;

use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, DependentJoin, Expr, ExprList,
    ExternColumnRefExpr, JoinType, LogOpExpr, LogOpType, LogicalAgg, LogicalFilter, LogicalJoin,
    LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;
use crate::rules::macros::define_rule;

/// Like rewrite_column_refs, except it translates ExternColumnRefs into ColumnRefs
fn rewrite_extern_column_refs(
    expr: &Expr,
    rewrite_fn: &mut impl FnMut(usize) -> Option<usize>,
) -> Option<Expr> {
    let expr_rel = expr.clone().into_rel_node();
    assert!(expr.typ().is_expression());
    if let OptRelNodeTyp::ExternColumnRef = expr.typ() {
        let col_ref = ExternColumnRefExpr::from_rel_node(expr_rel.clone()).unwrap();
        let rewritten = rewrite_fn(col_ref.index());
        return if let Some(rewritten_idx) = rewritten {
            let new_col_ref = ColumnRefExpr::new(rewritten_idx);
            Some(Expr::from_rel_node(new_col_ref.into_rel_node()).unwrap())
        } else {
            None
        };
    }

    let children = expr_rel.children.clone();
    let children = children
        .into_iter()
        .map(|child| {
            if child.typ == OptRelNodeTyp::List {
                // TODO: What should we do with List?
                return Some(child);
            }
            rewrite_extern_column_refs(&Expr::from_rel_node(child).unwrap(), rewrite_fn)
                .map(|x| x.into_rel_node())
        })
        .collect::<Option<Vec<_>>>()?;
    Some(
        Expr::from_rel_node(
            RelNode {
                typ: expr_rel.typ.clone(),
                children,
                data: expr_rel.data.clone(),
            }
            .into(),
        )
        .unwrap(),
    )
}

define_rule!(
    DepInitialDistinct,
    apply_dep_initial_distinct,
    (
        RawDepJoin(JoinType::Cross),
        left,
        right,
        [cond],
        [extern_cols]
    )
);

/// Initial rule to generate a join above this dependent join, and push the dependent
/// join further into the right side.
/// This is valuable because the left side of the dependent join will be a set, with only distinct values.
/// This makes later transformations valid, and has to happen first.
/// More information can be found in the "Unnesting Arbitrary Queries" paper.
fn apply_dep_initial_distinct(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    DepInitialDistinctPicks {
        left,
        right,
        cond,
        extern_cols,
    }: DepInitialDistinctPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());

    let left_schema_size = optimizer
        .get_property::<SchemaPropertyBuilder>(left.clone().into(), 0)
        .len();

    let right_schema_size = optimizer
        .get_property::<SchemaPropertyBuilder>(right.clone().into(), 0)
        .len();

    let correlated_col_indices = ExprList::from_rel_node(extern_cols.clone().into())
        .unwrap()
        .to_vec()
        .into_iter()
        .map(|x| {
            ExternColumnRefExpr::from_rel_node(x.into_rel_node())
                .unwrap()
                .index()
        })
        .collect::<Vec<usize>>();

    // If we have no correlated columns, just emit a cross join instead
    if correlated_col_indices.is_empty() {
        let new_join = LogicalJoin::new(
            PlanNode::from_group(left.into()),
            PlanNode::from_group(right.into()),
            ConstantExpr::bool(true).into_expr(),
            JoinType::Cross,
        );

        return vec![new_join.into_rel_node().as_ref().clone()];
    }

    // An aggregate node that groups by all correlated columns allows us to
    // effectively get the domain
    let distinct_agg_node = LogicalAgg::new(
        PlanNode::from_group(left.clone().into()),
        ExprList::new(vec![]),
        ExprList::new(
            correlated_col_indices
                .iter()
                .map(|x| ColumnRefExpr::new(*x).into_expr())
                .collect(),
        ),
    );

    let new_dep_join = DependentJoin::new(
        distinct_agg_node.into_plan_node(),
        PlanNode::from_group(right.into()),
        Expr::from_rel_node(cond.into()).unwrap(),
        ExprList::from_rel_node(extern_cols.into()).unwrap(),
        JoinType::Cross,
    );

    // Our join condition is going to make sure that all of the correlated columns
    // in the right side are equal to their equivalent columns in the left side.
    // (they will have the same index, just shifted over)
    let join_cond = LogOpExpr::new(
        LogOpType::And,
        ExprList::new(
            (0..correlated_col_indices.len())
                .map(|i| {
                    assert!(i + left_schema_size < left_schema_size + right_schema_size);
                    BinOpExpr::new(
                        ColumnRefExpr::new(i).into_expr(),
                        ColumnRefExpr::new(i + left_schema_size).into_expr(),
                        BinOpType::Eq,
                    )
                    .into_expr()
                })
                .collect(),
        ),
    )
    .into_expr();

    let new_join = LogicalJoin::new(
        PlanNode::from_group(left.into()),
        PlanNode::from_rel_node(new_dep_join.into_rel_node()).unwrap(),
        join_cond,
        JoinType::Inner,
    );

    // Ensure that the schema above the new_join is the same as it was before
    // for correctness (Project the left side of the new join,
    // plus the *right side of the right side*)
    let new_proj = LogicalProjection::new(
        PlanNode::from_rel_node(new_join.into_rel_node()).unwrap(),
        ExprList::new(
            (0..left_schema_size)
                .chain(
                    (left_schema_size + correlated_col_indices.len())
                        ..(left_schema_size + correlated_col_indices.len() + right_schema_size),
                )
                .map(|x| ColumnRefExpr::new(x).into_expr())
                .collect(),
        ),
    );

    vec![new_proj.into_rel_node().as_ref().clone()]
}

define_rule!(
    DepJoinPastProj,
    apply_dep_join_past_proj,
    (
        DepJoin(JoinType::Cross),
        left,
        (Projection, right, [exprs]),
        [cond],
        [extern_cols]
    )
);

/// Pushes a dependent join past a projection node.
/// The new projection node above the dependent join is changed to include the columns
/// from both sides of the dependent join. Otherwise, this transformation is trivial.
fn apply_dep_join_past_proj(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    DepJoinPastProjPicks {
        left,
        right,
        exprs: _,
        cond,
        extern_cols,
    }: DepJoinPastProjPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // TODO: can we have external columns in projection node? I don't think so?
    // Cross join should always have true cond
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());
    let left_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(left.clone().into(), 0)
        .len();
    let right_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(right.clone().into(), 0)
        .len();

    let right_cols_proj =
        (0..right_schema_len).map(|x| ColumnRefExpr::new(x + left_schema_len).into_expr());

    let left_cols_proj = (0..left_schema_len).map(|x| ColumnRefExpr::new(x).into_expr());
    let new_proj_exprs = ExprList::new(
        left_cols_proj
            .chain(right_cols_proj)
            .map(|x| x.into_expr())
            .collect(),
    );

    let new_dep_join = DependentJoin::new(
        PlanNode::from_group(left.into()),
        PlanNode::from_group(right.into()),
        Expr::from_rel_node(cond.into()).unwrap(),
        ExprList::from_rel_node(extern_cols.into()).unwrap(),
        JoinType::Cross,
    );
    let new_proj = LogicalProjection::new(
        PlanNode::from_rel_node(new_dep_join.into_rel_node()).unwrap(),
        new_proj_exprs,
    );

    vec![new_proj.into_rel_node().as_ref().clone()]
}

define_rule!(
    DepJoinPastFilter,
    apply_dep_join_past_filter,
    (
        DepJoin(JoinType::Cross),
        left,
        (Filter, right, [filter_cond]),
        [cond],
        [extern_cols]
    )
);

/// Pushes a dependent join past a projection node.
/// The new projection node above the dependent join is changed to include the columns
/// from both sides of the dependent join. Otherwise, this transformation is trivial.
fn apply_dep_join_past_filter(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    DepJoinPastFilterPicks {
        left,
        right,
        filter_cond,
        cond,
        extern_cols,
    }: DepJoinPastFilterPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Cross join should always have true cond
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());
    let left_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(left.clone().into(), 0)
        .len();

    let correlated_col_indices = ExprList::from_rel_node(extern_cols.clone().into())
        .unwrap()
        .to_vec()
        .into_iter()
        .map(|x| {
            ExternColumnRefExpr::from_rel_node(x.into_rel_node())
                .unwrap()
                .index()
        })
        .collect::<Vec<usize>>();

    let rewritten_expr = Expr::from_rel_node(filter_cond.into())
        .unwrap()
        .rewrite_column_refs(&mut |col| Some(col + left_schema_len))
        .unwrap();

    let rewritten_expr = rewrite_extern_column_refs(&rewritten_expr, &mut |col| {
        let idx = correlated_col_indices
            .iter()
            .position(|&x| x == col)
            .unwrap();
        Some(idx)
    })
    .unwrap();

    let new_dep_join = DependentJoin::new(
        PlanNode::from_group(left.into()),
        PlanNode::from_group(right.into()),
        Expr::from_rel_node(cond.into()).unwrap(),
        ExprList::new(
            correlated_col_indices
                .into_iter()
                .map(|x| ExternColumnRefExpr::new(x).into_expr())
                .collect(),
        ),
        JoinType::Cross,
    );

    let new_filter = LogicalFilter::new(
        PlanNode::from_rel_node(new_dep_join.into_rel_node()).unwrap(),
        rewritten_expr,
    );

    vec![new_filter.into_rel_node().as_ref().clone()]
}

define_rule!(
    DepJoinPastAgg,
    apply_dep_join_past_agg,
    (
        DepJoin(JoinType::Cross),
        left,
        (Agg, right, [exprs], [groups]),
        [cond],
        [extern_cols]
    )
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
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    DepJoinPastAggPicks {
        left,
        right,
        exprs,
        groups,
        cond,
        extern_cols,
    }: DepJoinPastAggPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Cross join should always have true cond
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());

    // TODO: OUTER JOIN TRANSFORMATION

    let extern_cols = ExprList::from_rel_node(extern_cols.into()).unwrap();
    let correlated_col_indices = extern_cols
        .to_vec()
        .into_iter()
        .map(|x| {
            ColumnRefExpr::new(
                ExternColumnRefExpr::from_rel_node(x.into_rel_node())
                    .unwrap()
                    .index(),
            )
            .into_expr()
        })
        .collect::<Vec<Expr>>();

    let groups = ExprList::from_rel_node(groups.clone().into()).unwrap();

    let new_groups = ExprList::new(
        groups
            .to_vec()
            .into_iter()
            .map(|x| {
                x.rewrite_column_refs(&mut |col| Some(col + correlated_col_indices.len()))
                    .unwrap()
            })
            .chain(correlated_col_indices.iter().map(|x| {
                x.rewrite_column_refs(&mut |col| Some(col + correlated_col_indices.len()))
                    .unwrap()
            }))
            .collect(),
    );

    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();

    let new_exprs = ExprList::new(
        exprs
            .to_vec()
            .into_iter()
            .map(|x| {
                x.rewrite_column_refs(&mut |col| Some(col + correlated_col_indices.len()))
                    .unwrap()
            })
            .collect(),
    );

    let new_dep_join = DependentJoin::new(
        PlanNode::from_group(left.into()),
        PlanNode::from_group(right.into()),
        Expr::from_rel_node(cond.into()).unwrap(),
        extern_cols,
        JoinType::Cross,
    );

    let new_agg = LogicalAgg::new(
        PlanNode::from_rel_node(new_dep_join.into_rel_node()).unwrap(),
        new_exprs,
        new_groups,
    );

    vec![new_agg.into_rel_node().as_ref().clone()]
}

// Heuristics-only rule. If we don't have references to the external columns on the right side,
// we can rewrite the dependent join into a normal join.
define_rule!(
    DepJoinEliminate,
    apply_dep_join_eliminate_at_scan, // TODO matching is all wrong
    (DepJoin(JoinType::Cross), left, right, [cond], [extern_cols])
);

/// If we've gone all the way down to the scan node, we can swap the dependent join
/// for an inner join! Our main mission is complete!
fn apply_dep_join_eliminate_at_scan(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    DepJoinEliminatePicks {
        left,
        right,
        cond,
        extern_cols: _,
    }: DepJoinEliminatePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Cross join should always have true cond
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());

    fn inspect(node: &RelNode<OptRelNodeTyp>) -> bool {
        if matches!(node.typ, OptRelNodeTyp::Placeholder(_)) {
            unimplemented!("this is a heuristics rule");
        }
        if node.typ == OptRelNodeTyp::ExternColumnRef {
            return false;
        }
        for child in &node.children {
            if !inspect(child) {
                return false;
            }
        }
        true
    }

    if inspect(&right) {
        let new_join = LogicalJoin::new(
            PlanNode::from_group(left.into()),
            PlanNode::from_group(right.into()),
            ConstantExpr::bool(true).into_expr(),
            JoinType::Inner,
        );
        vec![new_join.into_rel_node().as_ref().clone()]
    } else {
        vec![]
    }
}
