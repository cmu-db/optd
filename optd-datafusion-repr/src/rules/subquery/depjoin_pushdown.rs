use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};
use std::collections::HashMap;

use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, ExternColumnRefExpr,
    JoinType, LogOpExpr, LogOpType, LogicalAgg, LogicalDependentJoin, LogicalFilter, LogicalJoin,
    LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;
use crate::rules::macros::define_rule;

define_rule!(
    DepInitialDistinct,
    apply_dep_initial_distinct,
    (DepJoin(JoinType::Cross), left, right, [cond], [extern_cols])
);

static mut HAS_RUN: bool = false; // TODO: remove

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
    unsafe {
        // TODO: Awful approach here that doesn't work for more than 1 dep join in a query.
        // Figure something better out! Maybe a bool in the dependent join node?
        if HAS_RUN {
            return vec![];
        } else {
            HAS_RUN = true;
        }
    }

    let left_schema_size = optimizer
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

    let new_dep_join = LogicalDependentJoin::new(
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
            correlated_col_indices
                .into_iter()
                .map(|x| {
                    BinOpExpr::new(
                        ColumnRefExpr::new(x).into_expr(),
                        ColumnRefExpr::new(x + left_schema_size).into_expr(),
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

    vec![new_join.into_rel_node().as_ref().clone()]
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
        exprs,
        cond,
        extern_cols,
    }: DepJoinPastProjPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Cross join should always have true cond
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());
    let left_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(left.clone().into(), 0)
        .len();

    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();
    let right_cols_proj = exprs
        .to_vec()
        .into_iter()
        .map(|expr| {
            expr.rewrite_column_refs(&|col| Some(col + left_schema_len))
                .unwrap()
        })
        .collect::<Vec<Expr>>();

    let left_cols_proj = (0..left_schema_len).map(|x| ColumnRefExpr::new(x).into_expr());
    let new_proj_exprs = ExprList::new(
        left_cols_proj
            .chain(right_cols_proj)
            .map(|x| x.into_expr())
            .collect(),
    );

    let new_dep_join = LogicalDependentJoin::new(
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
    dbg!("Filter town\n");
    // Cross join should always have true cond
    assert!(cond == *ConstantExpr::bool(true).into_rel_node());
    let left_schema_len = optimizer
        .get_property::<SchemaPropertyBuilder>(left.clone().into(), 0)
        .len();

    let new_dep_join = LogicalDependentJoin::new(
        PlanNode::from_group(left.into()),
        PlanNode::from_group(right.into()),
        Expr::from_rel_node(cond.into()).unwrap(),
        ExprList::from_rel_node(extern_cols.into()).unwrap(),
        JoinType::Cross,
    );
    let new_filter = LogicalFilter::new(
        PlanNode::from_rel_node(new_dep_join.into_rel_node()).unwrap(),
        Expr::from_rel_node(filter_cond.into())
            .unwrap()
            .rewrite_column_refs(&|col| Some(col + left_schema_len))
            .unwrap(),
    );

    vec![new_filter.into_rel_node().as_ref().clone()]
}
