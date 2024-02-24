use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    Expr, ExprList, LogicalAgg, LogicalSort, OptRelNode, OptRelNodeTyp, PlanNode, SortOrderExpr,
    SortOrderType,
};

use super::macros::define_rule;

define_rule!(
    EliminateDuplicatedSortExprRule,
    apply_eliminate_duplicated_sort_expr,
    (Sort, child, [exprs])
);

/// Removes duplicate sort expressions
/// For exmaple:
///     select *
///     from t1
///     order by id desc, id, name, id asc
/// becomes
///     select *
///     from t1
///     order by id desc, name
fn apply_eliminate_duplicated_sort_expr(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateDuplicatedSortExprRulePicks { child, exprs }: EliminateDuplicatedSortExprRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let sort_keys: Vec<Expr> = exprs
        .children
        .iter()
        .map(|x| Expr::from_rel_node(x.clone()).unwrap())
        .collect_vec();

    let normalized_sort_keys: Vec<Arc<RelNode<OptRelNodeTyp>>> = exprs
        .children
        .iter()
        .map(|x| match x.typ {
            OptRelNodeTyp::SortOrder(_) => SortOrderExpr::new(
                SortOrderType::Asc,
                SortOrderExpr::from_rel_node(x.clone()).unwrap().child(),
            )
            .into_rel_node(),
            _ => x.clone(),
        })
        .collect_vec();

    let mut dedup_expr: Vec<Expr> = Vec::new();
    let mut dedup_set: HashSet<Arc<RelNode<OptRelNodeTyp>>> = HashSet::new();

    sort_keys
        .iter()
        .zip(normalized_sort_keys.iter())
        .for_each(|(expr, normalized_expr)| {
            if !dedup_set.contains(normalized_expr) {
                dedup_expr.push(expr.clone());
                dedup_set.insert(normalized_expr.to_owned());
            }
        });

    if dedup_expr.len() != sort_keys.len() {
        let node = LogicalSort::new(
            PlanNode::from_group(child.into()),
            ExprList::new(dedup_expr),
        );
        return vec![node.into_rel_node().as_ref().clone()];
    }
    vec![]
}

define_rule!(
    EliminateDuplicatedAggExprRule,
    apply_eliminate_duplicated_agg_expr,
    (Agg, child, exprs, [groups])
);

/// Removes duplicate group by expressions
/// For exmaple:
///     select *
///     from t1
///     group by id, name, id, id
/// becomes
///     select *
///     from t1
///     group by id, name
fn apply_eliminate_duplicated_agg_expr(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateDuplicatedAggExprRulePicks {
        child,
        exprs,
        groups,
    }: EliminateDuplicatedAggExprRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let mut dedup_expr: Vec<Expr> = Vec::new();
    let mut dedup_set: HashSet<Arc<RelNode<OptRelNodeTyp>>> = HashSet::new();
    groups.children.iter().for_each(|expr| {
        if !dedup_set.contains(expr) {
            dedup_expr.push(Expr::from_rel_node(expr.clone()).unwrap());
            dedup_set.insert(expr.clone());
        }
    });

    if dedup_expr.len() != groups.children.len() {
        let node = LogicalAgg::new(
            PlanNode::from_group(child.into()),
            ExprList::from_group(exprs.into()),
            ExprList::new(dedup_expr),
        );
        return vec![node.into_rel_node().as_ref().clone()];
    }
    vec![]
}
