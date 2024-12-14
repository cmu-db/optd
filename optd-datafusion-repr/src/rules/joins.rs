// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::vec;

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::{define_impl_rule, define_rule};
use crate::plan_nodes::{
    ArcDfPlanNode, BinOpPred, BinOpType, ColumnRefPred, ConstantPred, ConstantType, DfNodeType,
    DfPredType, DfReprPlanNode, DfReprPredNode, JoinType, ListPred, LogOpType,
    LogicalEmptyRelation, LogicalJoin, LogicalProjection, PhysicalHashJoin, PredExt,
};
use crate::properties::schema::Schema;
use crate::OptimizerExt;

// A join B -> B join A
define_rule!(
    JoinCommuteRule,
    apply_join_commute,
    (DfNodeType::Join(JoinType::Inner), left, right)
);

fn apply_join_commute(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = LogicalJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let left_schema = optimizer.get_schema_of(left.clone());
    let right_schema = optimizer.get_schema_of(right.clone());
    let cond = join
        .cond()
        .rewrite_column_refs(|idx| {
            Some(if idx < left_schema.len() {
                idx + right_schema.len()
            } else {
                idx - left_schema.len()
            })
        })
        .unwrap();
    let node = LogicalJoin::new_unchecked(right, left, cond, JoinType::Inner);
    let mut proj_expr = Vec::with_capacity(left_schema.len() + right_schema.len());
    for i in 0..left_schema.len() {
        proj_expr.push(ColumnRefPred::new(right_schema.len() + i).into_pred_node());
    }
    for i in 0..right_schema.len() {
        proj_expr.push(ColumnRefPred::new(i).into_pred_node());
    }
    let node = LogicalProjection::new(node.into_plan_node(), ListPred::new(proj_expr));
    vec![node.into_plan_node().into()]
}

define_rule!(
    EliminateJoinRule,
    apply_eliminate_join,
    (Join(JoinType::Inner), left, right)
);

/// Eliminate logical join with constant predicates
/// True predicates becomes CrossJoin (not yet implemented)
fn apply_eliminate_join(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = LogicalJoin::from_plan_node(binding).unwrap();
    let left = join.left();
    let right = join.right();
    let cond = join.cond();

    if let DfPredType::Constant(const_type) = cond.typ {
        if const_type == ConstantType::Bool {
            if let Some(ref data) = cond.data {
                if data.as_bool() {
                    let node = LogicalJoin::new_unchecked(
                        left,
                        right,
                        ConstantPred::bool(true).into_pred_node(),
                        JoinType::Inner,
                    );
                    return vec![node.into_plan_node().into()];
                } else {
                    // No need to handle schema here, as all exprs in the same group
                    // will have same logical properties
                    let mut left_fields = optimizer.get_schema_of(left.clone()).fields;
                    let right_fields = optimizer.get_schema_of(right.clone()).fields;
                    left_fields.extend(right_fields);
                    let new_schema = Schema {
                        fields: left_fields,
                    };
                    let node = LogicalEmptyRelation::new(false, new_schema);
                    return vec![node.into_plan_node().into()];
                }
            }
        }
    }
    vec![]
}

// // (A join B) join C -> A join (B join C)
define_rule!(
    JoinAssocRule,
    apply_join_assoc,
    (Join(JoinType::Inner), (Join(JoinType::Inner), a, b), c)
);

fn apply_join_assoc(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join1: LogicalJoin = LogicalJoin::from_plan_node(binding).unwrap();
    let c = join1.right();
    let join2 = LogicalJoin::from_plan_node(join1.left().unwrap_plan_node()).unwrap();
    let a = join2.left();
    let b = join2.right();
    let cond1 = join2.cond();
    let a_schema = optimizer.get_schema_of(a.clone());
    let cond2 = join1.cond();

    let Some(cond2) = cond2.rewrite_column_refs(&mut |idx| {
        if idx < a_schema.len() {
            None
        } else {
            Some(idx - a_schema.len())
        }
    }) else {
        return vec![];
    };
    let node = LogicalJoin::new_unchecked(
        a,
        LogicalJoin::new_unchecked(b, c, cond2, JoinType::Inner).into_plan_node(),
        cond1,
        JoinType::Inner,
    );
    vec![node.into_plan_node().into()]
}

define_impl_rule!(
    HashJoinRule,
    apply_hash_join,
    (Join(JoinType::Inner), left, right)
);

fn apply_hash_join(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = LogicalJoin::from_plan_node(binding).unwrap();
    let cond = join.cond();
    let left = join.left();
    let right = join.right();
    match cond.typ {
        DfPredType::BinOp(BinOpType::Eq) => {
            let left_schema = optimizer.get_schema_of(left.clone());
            let op = BinOpPred::from_pred_node(cond.clone()).unwrap();
            let left_expr = op.left_child();
            let right_expr = op.right_child();
            let Some(mut left_expr) = ColumnRefPred::from_pred_node(left_expr) else {
                return vec![];
            };
            let Some(mut right_expr) = ColumnRefPred::from_pred_node(right_expr) else {
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
                let right_expr = ColumnRefPred::new(right_expr.index() - left_schema.len());
                let node = PhysicalHashJoin::new_unchecked(
                    left,
                    right,
                    ListPred::new(vec![left_expr.into_pred_node()]),
                    ListPred::new(vec![right_expr.into_pred_node()]),
                    JoinType::Inner,
                );
                return vec![node.into_plan_node().into()];
            }
        }
        DfPredType::LogOp(LogOpType::And) => {
            // currently only support consecutive equal queries
            let mut is_consecutive_eq = true;
            for child in cond.children.clone() {
                if let DfPredType::BinOp(BinOpType::Eq) = child.typ {
                    continue;
                } else {
                    is_consecutive_eq = false;
                    break;
                }
            }
            if !is_consecutive_eq {
                return vec![];
            }

            let left_schema = optimizer.get_schema_of(left.clone());
            let mut left_exprs = vec![];
            let mut right_exprs = vec![];
            for child in &cond.children {
                let bin_op = BinOpPred::from_pred_node(child.clone()).unwrap();
                let left_expr = bin_op.left_child();
                let right_expr = bin_op.right_child();
                let Some(mut left_expr) = ColumnRefPred::from_pred_node(left_expr) else {
                    return vec![];
                };
                let Some(mut right_expr) = ColumnRefPred::from_pred_node(right_expr) else {
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
                let right_expr = ColumnRefPred::new(right_expr.index() - left_schema.len());
                right_exprs.push(right_expr.into_pred_node());
                left_exprs.push(left_expr.into_pred_node());
            }

            let node = PhysicalHashJoin::new_unchecked(
                left,
                right,
                ListPred::new(left_exprs),
                ListPred::new(right_exprs),
                JoinType::Inner,
            );
            return vec![node.into_plan_node().into()];
        }
        _ => {}
    }
    vec![]
}
