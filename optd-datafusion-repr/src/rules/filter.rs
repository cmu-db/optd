use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::plan_nodes::{
    ConstantExpr, ConstantType, Expr, ExprList, LogOpExpr, LogOpType, LogicalEmptyRelation,
    OptRelNode, OptRelNodeTyp,
};
use crate::OptRelNodeRef;
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use super::macros::define_rule;

define_rule!(
    SimplifyFilterRule,
    apply_simplify_filter,
    (Filter, child, [cond])
);

// simplify_log_expr simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
fn simplify_log_expr(log_expr: OptRelNodeRef, changed: &mut bool) -> OptRelNodeRef {
    let log_expr = LogOpExpr::from_rel_node(log_expr).unwrap();
    let op = log_expr.op_type();
    // we need a new children vec to output deterministic order
    let mut new_children_set = HashSet::new();
    let mut new_children = Vec::new();
    let children_size = log_expr.children().len();
    for child in log_expr.children() {
        let mut new_child = child;
        if let OptRelNodeTyp::LogOp(_) = new_child.typ() {
            let new_expr = simplify_log_expr(new_child.into_rel_node().clone(), changed);
            new_child = Expr::from_rel_node(new_expr).unwrap();
        }
        if let OptRelNodeTyp::Constant(ConstantType::Bool) = new_child.typ() {
            let data = new_child.into_rel_node().data.clone().unwrap();
            *changed = true;
            // TrueExpr
            if data.as_bool() {
                if op == LogOpType::And {
                    // skip True in And
                    continue;
                }
                if op == LogOpType::Or {
                    // replace whole exprList with True
                    return ConstantExpr::bool(true).into_rel_node().clone();
                }
                unreachable!("no other type in logOp");
            }
            // FalseExpr
            if op == LogOpType::And {
                // replace whole exprList with False
                return ConstantExpr::bool(false).into_rel_node().clone();
            }
            if op == LogOpType::Or {
                // skip False in Or
                continue;
            }
            unreachable!("no other type in logOp");
        } else if !new_children_set.contains(&new_child) {
            new_children_set.insert(new_child.clone());
            new_children.push(new_child);
        }
    }
    if new_children.is_empty() {
        if op == LogOpType::And {
            return ConstantExpr::bool(true).into_rel_node().clone();
        }
        if op == LogOpType::Or {
            return ConstantExpr::bool(false).into_rel_node().clone();
        }
        unreachable!("no other type in logOp");
    }
    if new_children.len() == 1 {
        *changed = true;
        return new_children
            .into_iter()
            .next()
            .unwrap()
            .into_rel_node()
            .clone();
    }
    if children_size != new_children.len() {
        *changed = true;
    }
    LogOpExpr::new(op, ExprList::new(new_children))
        .into_rel_node()
        .clone()
}

// SimplifySelectFilters simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
fn apply_simplify_filter(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    SimplifyFilterRulePicks { child, cond }: SimplifyFilterRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    match cond.typ {
        OptRelNodeTyp::LogOp(_) => {
            let mut changed = false;
            let new_log_expr = simplify_log_expr(Arc::new(cond), &mut changed);
            if changed {
                let filter_node = RelNode {
                    typ: OptRelNodeTyp::Filter,
                    children: vec![child.into(), new_log_expr],
                    data: None,
                };
                return vec![filter_node];
            }
            vec![]
        }
        _ => {
            vec![]
        }
    }
}

define_rule!(
    EliminateFilterRule,
    apply_eliminate_filter,
    (Filter, child, [cond])
);

/// Transformations:
///     - Filter node w/ false pred -> EmptyRelation
///     - Filter node w/ true pred  -> Eliminate from the tree
fn apply_eliminate_filter(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateFilterRulePicks { child, cond }: EliminateFilterRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::Constant(ConstantType::Bool) = cond.typ {
        if let Some(data) = cond.data {
            if data.as_bool() {
                // If the condition is true, eliminate the filter node, as it
                // will yield everything from below it.
                return vec![child];
            } else {
                // If the condition is false, replace this node with the empty relation,
                // since it will never yield tuples.
                let node = LogicalEmptyRelation::new(false);
                return vec![node.into_rel_node().as_ref().clone()];
            }
        }
    }
    vec![]
}
