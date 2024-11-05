use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::{MaybeRelNode, RelNode};
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::{define_impl_rule, define_rule};
use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, ExprList, JoinType,
    LogOpType, LogicalEmptyRelation, LogicalJoin, LogicalProjection, OptRelNode, OptRelNodeTyp,
    PhysicalHashJoin, PlanNode,
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
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(right.clone(), 0);
    let cond = Expr::ensures_interpret(cond).rewrite_column_refs(&mut |idx| {
        Some(if idx < left_schema.len() {
            idx + right_schema.len()
        } else {
            idx - left_schema.len()
        })
    });
    let node = LogicalJoin::new(
        PlanNode::interpret(right),
        PlanNode::interpret(left),
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
    let node = LogicalProjection::new(node.into_plan_node(), ExprList::new(proj_expr));
    vec![node.strip()]
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
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    let cond = cond.unwrap_rel_node();
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
                    return vec![node.strip()];
                } else {
                    // No need to handle schema here, as all exprs in the same group
                    // will have same logical properties
                    let mut left_fields = optimizer
                        .get_property::<SchemaPropertyBuilder>(left.clone(), 0)
                        .fields;
                    let right_fields = optimizer
                        .get_property::<SchemaPropertyBuilder>(right.clone(), 0)
                        .fields;
                    left_fields.extend(right_fields);
                    let new_schema = Schema {
                        fields: left_fields,
                    };
                    let node = LogicalEmptyRelation::new(false, new_schema);
                    return vec![node.strip()];
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
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    let a_schema = optimizer.get_property::<SchemaPropertyBuilder>(a.clone(), 0);
    let _b_schema = optimizer.get_property::<SchemaPropertyBuilder>(b.clone(), 0);
    let _c_schema = optimizer.get_property::<SchemaPropertyBuilder>(c.clone(), 0);

    let cond2 = Expr::ensures_interpret(cond2);

    let Some(cond2) = cond2.rewrite_column_refs(&mut |idx| {
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
                children: vec![b.into(), c.into(), cond2.strip()],
                data: None,
                predicates: Vec::new(), /* TODO: refactor */
            }
            .into(),
            cond1.into(),
        ],
        data: None,
        predicates: Vec::new(), /* TODO: refactor */
    };
    vec![node.into()]
}

define_impl_rule!(
    HashJoinRule,
    apply_hash_join,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_hash_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    HashJoinRulePicks { left, right, cond }: HashJoinRulePicks,
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    let cond = cond.unwrap_rel_node();
    match cond.typ {
        OptRelNodeTyp::BinOp(BinOpType::Eq) => {
            let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
            let op = BinOpExpr::ensures_interpret(cond.clone());
            let left_expr = op.left_child();
            let right_expr = op.right_child();
            let Some(mut left_expr) = ColumnRefExpr::try_interpret(left_expr.strip()) else {
                return vec![];
            };
            let Some(mut right_expr) = ColumnRefExpr::try_interpret(right_expr.strip()) else {
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
                    ExprList::new(vec![left_expr.into_expr()]),
                    ExprList::new(vec![right_expr.into_expr()]),
                    JoinType::Inner,
                );
                return vec![node.strip()];
            }
        }
        OptRelNodeTyp::LogOp(LogOpType::And) => {
            // currently only support consecutive equal queries
            let mut is_consecutive_eq = true;
            for child in cond.children.clone() {
                if let OptRelNodeTyp::BinOp(BinOpType::Eq) = child.unwrap_typ() {
                    continue;
                } else {
                    is_consecutive_eq = false;
                    break;
                }
            }
            if !is_consecutive_eq {
                return vec![];
            }

            let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
            let mut left_exprs = vec![];
            let mut right_exprs = vec![];
            for child in cond.children {
                let bin_op = BinOpExpr::ensures_interpret(child);
                let left_expr: Expr = bin_op.left_child();
                let right_expr = bin_op.right_child();
                let Some(mut left_expr) = ColumnRefExpr::try_interpret(left_expr.strip()) else {
                    return vec![];
                };
                let Some(mut right_expr) = ColumnRefExpr::try_interpret(right_expr.strip()) else {
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
                ExprList::new(left_exprs),
                ExprList::new(right_exprs),
                JoinType::Inner,
            );
            return vec![node.strip()];
        }
        _ => {}
    }
    if let OptRelNodeTyp::BinOp(BinOpType::Eq) = cond.typ {
        let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
        let op = BinOpExpr::ensures_interpret(cond);
        let left_expr: Expr = op.left_child();
        let right_expr = op.right_child();
        let Some(mut left_expr) = ColumnRefExpr::try_interpret(left_expr.strip()) else {
            return vec![];
        };
        let Some(mut right_expr) = ColumnRefExpr::try_interpret(right_expr.strip()) else {
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
                ExprList::new(vec![left_expr.into_expr()]),
                ExprList::new(vec![right_expr.into_expr()]),
                JoinType::Inner,
            );
            return vec![node.strip()];
        }
    }
    vec![]
}
