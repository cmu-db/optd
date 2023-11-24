use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::{define_impl_rule, define_rule};
use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, JoinType, LogicalJoin,
    LogicalProjection, OptRelNode, OptRelNodeTyp, PhysicalHashJoin, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;

define_rule!(
    JoinCommuteRule,
    apply_join_commute,
    (Join(JoinType::Inner), left, right, [cond])
);

fn rewrite_column_refs(expr: Expr, left_size: usize, right_size: usize) -> Expr {
    let expr = expr.into_rel_node();
    if let Some(expr) = ColumnRefExpr::from_rel_node(expr.clone()) {
        let index = expr.index();
        if index < left_size {
            return ColumnRefExpr::new(index + right_size).into_expr();
        } else {
            return ColumnRefExpr::new(index - left_size).into_expr();
        }
    }
    let children = expr.children.clone();
    let children = children
        .into_iter()
        .map(|x| {
            rewrite_column_refs(Expr::from_rel_node(x).unwrap(), left_size, right_size)
                .into_rel_node()
        })
        .collect_vec();
    Expr::from_rel_node(
        RelNode {
            typ: expr.typ.clone(),
            children,
            data: expr.data.clone(),
        }
        .into(),
    )
    .unwrap()
}

fn apply_join_commute(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinCommuteRulePicks { left, right, cond }: JoinCommuteRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0);
    let cond = rewrite_column_refs(
        Expr::from_rel_node(cond.into()).unwrap(),
        left_schema.len(),
        right_schema.len(),
    );
    let node = LogicalJoin::new(
        PlanNode::from_group(right.into()),
        PlanNode::from_group(left.into()),
        cond,
        JoinType::Inner,
    );
    let mut proj_expr = Vec::with_capacity(left_schema.0.len() + right_schema.0.len());
    for i in 0..left_schema.len() {
        proj_expr.push(ColumnRefExpr::new(right_schema.len() + i).into_expr());
    }
    for i in 0..right_schema.len() {
        proj_expr.push(ColumnRefExpr::new(i).into_expr());
    }
    let node =
        LogicalProjection::new(node.into_plan_node(), ExprList::new(proj_expr)).into_rel_node();
    vec![node.as_ref().clone()]
}

define_rule!(
    JoinAssocRule,
    apply_join_assoc,
    (
        Join(JoinType::Inner),
        (Join(JoinType::Inner), a, b, cond1),
        c,
        cond2
    )
);

fn apply_join_assoc(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinAssocRulePicks {
        a,
        b,
        c,
        cond1,
        cond2,
    }: JoinAssocRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let node = RelNode {
        typ: OptRelNodeTyp::Join(JoinType::Inner),
        children: vec![
            a.into(),
            RelNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                children: vec![b.into(), c.into(), cond2.into()],
                data: None,
            }
            .into(),
            cond1.into(),
        ],
        data: None,
    };
    vec![node]
}

define_impl_rule!(
    HashJoinRule,
    apply_hash_join,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_hash_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    HashJoinRulePicks { left, right, cond }: HashJoinRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::BinOp(BinOpType::Eq) = cond.typ {
        let left_schema =
            optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
        // let right_schema =
        //     optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0);
        let op = BinOpExpr::from_rel_node(Arc::new(cond.clone())).unwrap();
        let left_expr: Expr = op.left_child();
        let right_expr = op.right_child();
        let Some(mut left_expr) = ColumnRefExpr::from_rel_node(left_expr.into_rel_node()) else {
            return vec![];
        };
        let Some(mut right_expr) = ColumnRefExpr::from_rel_node(right_expr.into_rel_node()) else {
            return vec![];
        };
        let can_convert = if left_expr.index() < left_schema.0.len()
            && right_expr.index() >= left_schema.0.len()
        {
            true
        } else if right_expr.index() < left_schema.0.len()
            && left_expr.index() >= left_schema.0.len()
        {
            (left_expr, right_expr) = (right_expr, left_expr);
            true
        } else {
            false
        };

        if can_convert {
            let right_expr = ColumnRefExpr::new(right_expr.index() - left_schema.0.len());
            let node = PhysicalHashJoin::new(
                PlanNode::from_group(left.into()),
                PlanNode::from_group(right.into()),
                ExprList::new(vec![left_expr.into_expr()]),
                ExprList::new(vec![right_expr.into_expr()]),
                JoinType::Inner,
            );
            return vec![node.into_rel_node().as_ref().clone()];
        }
    }
    vec![]
}
