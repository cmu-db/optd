use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};
use std::collections::HashMap;

use crate::plan_nodes::{
    ColumnRefExpr, ConstantExpr, Expr, ExprList, JoinType, LogicalDependentJoin, LogicalProjection,
    OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;
use crate::rules::macros::define_rule;

define_rule!(
    DepJoinPastProj,
    apply_dep_join_past_proj,
    (
        DepJoin(JoinType::Cross),
        left,
        (Projection, right, [exprs]),
        [cond]
    )
);

fn apply_dep_join_past_proj(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    DepJoinPastProjPicks {
        left,
        right,
        exprs,
        cond,
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
        JoinType::Cross,
    );
    let new_proj = LogicalProjection::new(
        PlanNode::from_rel_node(new_dep_join.into_rel_node()).unwrap(),
        new_proj_exprs,
    );

    vec![new_proj.into_rel_node().as_ref().clone()]
}
