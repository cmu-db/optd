use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::{define_impl_rule, define_rule};
use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, ExprList, JoinType,
    LogOpType, LogicalEmptyRelation, LogicalJoin, LogicalProjection, OptRelNode, OptRelNodeTyp,
    PhysicalExprList, PhysicalHashJoin, PlanNode,
};
use crate::properties::schema::{Schema, SchemaPropertyBuilder};

// A join B -> B join A
define_rule!(
    JoinCommuteRule,
    apply_join_commute,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_join_commute(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinCommuteRulePicks { left, right, cond }: JoinCommuteRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0);
    let cond = Expr::from_rel_node(cond.into())
        .unwrap()
        .rewrite_column_refs(&|idx| {
            Some(if idx < left_schema.len() {
                idx + right_schema.len()
            } else {
                idx - left_schema.len()
            })
        });
    let node = LogicalJoin::new(
        PlanNode::from_group(right.into()),
        PlanNode::from_group(left.into()),
        cond.unwrap(),
        JoinType::Inner,
    );
    let mut proj_expr = Vec::with_capacity(left_schema.len() + right_schema.len());
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
    EliminateJoinRule,
    apply_eliminate_join,
    (Join(JoinType::Inner), left, right, [cond])
);

/// Eliminate logical join with constant predicates
/// True predicates becomes CrossJoin (not yet implemented)
#[allow(unused_variables)]
fn apply_eliminate_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateJoinRulePicks { left, right, cond }: EliminateJoinRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::Constant(const_type) = cond.typ {
        if const_type == ConstantType::Bool {
            if let Some(data) = cond.data {
                if data.as_bool() {
                    // change it to cross join if filter is always true
                    let node = LogicalJoin::new(
                        PlanNode::from_group(left.into()),
                        PlanNode::from_group(right.into()),
                        ConstantExpr::bool(true).into_expr(),
                        JoinType::Cross,
                    );
                    return vec![node.into_rel_node().as_ref().clone()];
                } else {
                    // No need to handle schema here, as all exprs in the same group
                    // will have same logical properties
                    let mut left_fields = optimizer
                        .get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0)
                        .fields;
                    let right_fields = optimizer
                        .get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0)
                        .fields;
                    left_fields.extend(right_fields);
                    let new_schema = Schema {
                        fields: left_fields,
                    };
                    let node = LogicalEmptyRelation::new(false, new_schema);
                    return vec![node.into_rel_node().as_ref().clone()];
                }
            }
        }
    }
    vec![]
}

// (A join B) join C -> A join (B join C)
define_rule!(
    JoinAssocRule,
    apply_join_assoc,
    (
        Join(JoinType::Inner),
        (Join(JoinType::Inner), a, b, [cond1]),
        c,
        [cond2]
    )
);

fn apply_join_assoc(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinAssocRulePicks {
        a,
        b,
        c,
        cond1,
        cond2,
    }: JoinAssocRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let a_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(a.clone()), 0);
    let _b_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(b.clone()), 0);
    let _c_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(c.clone()), 0);

    let cond2 = Expr::from_rel_node(cond2.into()).unwrap();

    let Some(cond2) = cond2.rewrite_column_refs(&|idx| {
        if idx < a_schema.len() {
            None
        } else {
            Some(idx - a_schema.len())
        }
    }) else {
        return vec![];
    };

    let node = RelNode {
        typ: OptRelNodeTyp::Join(JoinType::Inner),
        children: vec![
            a.into(),
            RelNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                children: vec![b.into(), c.into(), cond2.into_rel_node()],
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
    match cond.typ {
        OptRelNodeTyp::BinOp(BinOpType::Eq) => {
            let left_schema =
                optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
            // let right_schema =
            //     optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0);
            let op = BinOpExpr::from_rel_node(Arc::new(cond.clone())).unwrap();
            let left_expr: Expr = op.left_child();
            let right_expr = op.right_child();
            let Some(mut left_expr) = ColumnRefExpr::from_rel_node(left_expr.into_rel_node())
            else {
                return vec![];
            };
            let Some(mut right_expr) = ColumnRefExpr::from_rel_node(right_expr.into_rel_node())
            else {
                return vec![];
            };
            let can_convert = if left_expr.index() < left_schema.len()
                && right_expr.index() >= left_schema.len()
            {
                true
            } else if right_expr.index() < left_schema.len()
                && left_expr.index() >= left_schema.len()
            {
                (left_expr, right_expr) = (right_expr, left_expr);
                true
            } else {
                false
            };

            if can_convert {
                let right_expr = ColumnRefExpr::new(right_expr.index() - left_schema.len());
                let node = PhysicalHashJoin::new(
                    PlanNode::from_group(left.into()),
                    PlanNode::from_group(right.into()),
                    ExprList::new(vec![left_expr.into_expr()]).into_expr(),
                    ExprList::new(vec![right_expr.into_expr()]).into_expr(),
                    JoinType::Inner,
                );
                return vec![node.into_rel_node().as_ref().clone()];
            }
        }
        OptRelNodeTyp::LogOp(LogOpType::And) => {
            // currently only support consecutive equal queries
            let mut is_consecutive_eq = true;
            for child in cond.children.clone() {
                if let OptRelNodeTyp::BinOp(BinOpType::Eq) = child.typ {
                    continue;
                } else {
                    is_consecutive_eq = false;
                    break;
                }
            }
            if !is_consecutive_eq {
                return vec![];
            }

            let left_schema =
                optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
            let mut left_exprs = vec![];
            let mut right_exprs = vec![];
            for child in cond.children {
                let bin_op = BinOpExpr::from_rel_node(child.clone()).unwrap();
                let left_expr: Expr = bin_op.left_child();
                let right_expr = bin_op.right_child();
                let Some(mut left_expr) = ColumnRefExpr::from_rel_node(left_expr.into_rel_node())
                else {
                    return vec![];
                };
                let Some(mut right_expr) = ColumnRefExpr::from_rel_node(right_expr.into_rel_node())
                else {
                    return vec![];
                };
                let can_convert = if left_expr.index() < left_schema.len()
                    && right_expr.index() >= left_schema.len()
                {
                    true
                } else if right_expr.index() < left_schema.len()
                    && left_expr.index() >= left_schema.len()
                {
                    (left_expr, right_expr) = (right_expr, left_expr);
                    true
                } else {
                    false
                };
                if !can_convert {
                    return vec![];
                }
                let right_expr = ColumnRefExpr::new(right_expr.index() - left_schema.len());
                right_exprs.push(right_expr.into_expr());
                left_exprs.push(left_expr.into_expr());
            }

            let node = PhysicalHashJoin::new(
                PlanNode::from_group(left.into()),
                PlanNode::from_group(right.into()),
                ExprList::new(left_exprs).into_expr(),
                ExprList::new(right_exprs).into_expr(),
                JoinType::Inner,
            );
            return vec![node.into_rel_node().as_ref().clone()];
        }
        _ => {}
    }
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
        let can_convert = if left_expr.index() < left_schema.len()
            && right_expr.index() >= left_schema.len()
        {
            true
        } else if right_expr.index() < left_schema.len() && left_expr.index() >= left_schema.len() {
            (left_expr, right_expr) = (right_expr, left_expr);
            true
        } else {
            false
        };

        if can_convert {
            let right_expr = ColumnRefExpr::new(right_expr.index() - left_schema.len());
            let node = PhysicalHashJoin::new(
                PlanNode::from_group(left.into()),
                PlanNode::from_group(right.into()),
                ExprList::new(vec![left_expr.into_expr()]).into_expr(),
                ExprList::new(vec![right_expr.into_expr()]).into_expr(),
                JoinType::Inner,
            );
            return vec![node.into_rel_node().as_ref().clone()];
        }
    }
    vec![]
}
