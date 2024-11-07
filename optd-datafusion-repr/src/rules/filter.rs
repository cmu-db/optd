use std::collections::HashSet;

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::define_rule;
use crate::plan_nodes::{
    ArcDfPredNode, ConstantPred, ConstantType, DfNodeType, DfPredType, DfReprPlanNode,
    DfReprPredNode, JoinType, LogOpPred, LogOpType, LogicalEmptyRelation, LogicalFilter,
    LogicalJoin,
};
use crate::{ArcDfPlanNode, OptimizerExt};

// simplify_log_expr simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
pub(crate) fn simplify_log_expr(log_expr: ArcDfPredNode, changed: &mut bool) -> ArcDfPredNode {
    let log_expr = LogOpPred::from_pred_node(log_expr).unwrap();
    let op = log_expr.op_type();
    // we need a new children vec to output deterministic order
    let mut new_children_set = HashSet::new();
    let mut new_children = Vec::new();
    let log_expr_children = log_expr.children();
    let children_size = log_expr_children.len();
    for child in log_expr_children {
        let mut new_child = child;
        if let DfPredType::LogOp(_) = new_child.typ {
            new_child = simplify_log_expr(new_child, changed);
        }
        if let DfPredType::Constant(ConstantType::Bool) = new_child.typ {
            let data = ConstantPred::from_pred_node(new_child).unwrap().value();
            *changed = true;
            // TrueExpr
            if data.as_bool() {
                if op == LogOpType::And {
                    // skip True in And
                    continue;
                }
                if op == LogOpType::Or {
                    // replace whole exprList with True
                    return ConstantPred::bool(true).into_pred_node();
                }
                unreachable!("no other type in logOp");
            }
            // FalseExpr
            if op == LogOpType::And {
                // replace whole exprList with False
                return ConstantPred::bool(false).into_pred_node();
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
            return ConstantPred::bool(true).into_pred_node();
        }
        if op == LogOpType::Or {
            return ConstantPred::bool(false).into_pred_node();
        }
        unreachable!("no other type in logOp");
    }
    if new_children.len() == 1 {
        *changed = true;
        return new_children.first().cloned().unwrap().into_pred_node();
    }
    if children_size != new_children.len() {
        *changed = true;
    }
    LogOpPred::new(op, new_children).into_pred_node()
}

define_rule!(SimplifyFilterRule, apply_simplify_filter, (Filter, child));

// SimplifySelectFilters simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
fn apply_simplify_filter(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let cond = filter.cond();
    match cond.typ {
        DfPredType::LogOp(_) => {
            let mut changed = false;
            let new_log_expr = simplify_log_expr(cond, &mut changed);
            if changed {
                let filter_node = LogicalFilter::new_unchecked(filter.child(), new_log_expr);
                return vec![filter_node.into_plan_node().into()];
            }
            vec![]
        }
        _ => {
            vec![]
        }
    }
}

// Same as SimplifyFilterRule, but for innerJoin conditions
define_rule!(
    SimplifyJoinCondRule,
    apply_simplify_join_cond,
    (Join(JoinType::Inner), left, right)
);

fn apply_simplify_join_cond(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = LogicalJoin::from_plan_node(binding).unwrap();
    let cond = join.cond();
    let left = join.left();
    let right = join.right();

    match cond.typ {
        DfPredType::LogOp(_) => {
            let mut changed = false;
            let new_log_expr = simplify_log_expr(cond, &mut changed);
            if changed {
                let join_node =
                    LogicalJoin::new_unchecked(left, right, new_log_expr, JoinType::Inner);
                return vec![join_node.into_plan_node().into()];
            }
            vec![]
        }
        _ => {
            vec![]
        }
    }
}

define_rule!(EliminateFilterRule, apply_eliminate_filter, (Filter, child));

/// Transformations:
///     - Filter node w/ false pred -> EmptyRelation
///     - Filter node w/ true pred  -> Eliminate from the tree
fn apply_eliminate_filter(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let cond = filter.cond();
    if let DfPredType::Constant(ConstantType::Bool) = cond.typ {
        if let Some(ref data) = cond.data {
            if data.as_bool() {
                // If the condition is true, eliminate the filter node, as it
                // will yield everything from below it.
                return vec![filter.child()];
            } else {
                // If the condition is false, replace this node with the empty relation,
                // since it will never yield tuples.
                let schema = optimizer.get_schema_of(filter.child());
                let node = LogicalEmptyRelation::new(false, schema);
                return vec![node.into_plan_node().into()];
            }
        }
    }
    vec![]
}
