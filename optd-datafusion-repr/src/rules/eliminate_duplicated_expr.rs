use std::collections::HashSet;

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::define_rule;
use crate::plan_nodes::{
    ArcDfPlanNode, DfNodeType, DfReprPlanNode, DfReprPredNode, ListPred, LogicalAgg, LogicalSort,
    SortOrderPred,
};

define_rule!(
    EliminateDuplicatedSortExprRule,
    apply_eliminate_duplicated_sort_expr,
    (Sort, child)
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
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let sort = LogicalSort::from_plan_node(binding).unwrap();
    let exprs = sort.exprs();
    let sort_keys = exprs.to_vec().into_iter();

    let mut dedup_expr = Vec::new();
    let mut dedup_set = HashSet::new();
    let mut deduped = false;

    for sort_key in sort_keys {
        let sort_expr = SortOrderPred::from_pred_node(sort_key.clone()).unwrap();
        if !dedup_set.contains(&sort_expr.child()) {
            dedup_expr.push(sort_key.clone());
            dedup_set.insert(sort_expr.child().clone());
        } else {
            deduped = true;
        }
    }

    if deduped {
        let node = LogicalSort::new_unchecked(sort.child(), ListPred::new(dedup_expr));
        return vec![node.into_plan_node().into()];
    }
    vec![]
}

define_rule!(
    EliminateDuplicatedAggExprRule,
    apply_eliminate_duplicated_agg_expr,
    (Agg, child)
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
///
/// TODO: if projection refers to the column, we need to update the projection
fn apply_eliminate_duplicated_agg_expr(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let agg = LogicalAgg::from_plan_node(binding).unwrap();
    let groups = agg.groups().to_vec();

    let mut dedup_expr = Vec::new();
    let mut dedup_set = HashSet::new();
    let mut deduped = false;

    for group in groups {
        if !dedup_set.contains(&group) {
            dedup_expr.push(group.clone());
            dedup_set.insert(group.clone());
        } else {
            deduped = true;
        }
    }

    if deduped {
        let node = LogicalAgg::new_unchecked(agg.child(), agg.exprs(), ListPred::new(dedup_expr));
        return vec![node.into_plan_node().into()];
    }
    vec![]
}
