// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! This rule is designed to be applied heuristically (read: all the time, blindly).
//! However, pushing a filter is not *always* better (but it usually is). If cost is
//! to be taken into account, each transposition step can be done separately
//! (and are thus all in independent functions).
//! One can even implement each of these helper functions as their own transpose rule,
//! like Calcite does.
//!
//! At a high level, filter pushdown is responsible for pushing the filter node
//! further down the query plan whenever it is possible to do so.

// TODO: Separate filter transpositions into several files like proj transpose

use core::panic;
use std::collections::HashSet;
use std::vec;

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::filter::simplify_log_expr;
use super::macros::define_rule;
use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, ColumnRefPred, DfNodeType, DfPredType, DfReprPlanNode,
    DfReprPredNode, JoinType, ListPred, LogOpPred, LogOpType, LogicalAgg, LogicalFilter,
    LogicalJoin, LogicalSort, PredExt,
};
use crate::OptimizerExt;

/// Emits a LogOpExpr AND if the list has more than one element
/// Otherwise, returns the single element
fn and_expr_list_to_expr(exprs: Vec<ArcDfPredNode>) -> ArcDfPredNode {
    if exprs.len() == 1 {
        exprs.first().unwrap().clone()
    } else {
        LogOpPred::new(LogOpType::And, exprs).into_pred_node()
    }
}

fn merge_conds(first: ArcDfPredNode, second: ArcDfPredNode) -> ArcDfPredNode {
    let new_expr_list = ListPred::new(vec![first, second]);
    // Flatten nested logical expressions if possible
    let flattened =
        LogOpPred::new_flattened_nested_logical(LogOpType::And, new_expr_list).into_pred_node();
    let mut changed = false;
    // TODO: such simplifications should be invoked from optd-core, instead of ad-hoc
    simplify_log_expr(flattened, &mut changed)
}

#[derive(Debug, Clone, Copy)]
enum JoinCondDependency {
    Left,
    Right,
    Both,
    None,
}

/// Given a list of expressions (presumably a flattened tree), determine
/// if the expression is dependent on the left child, the right child, both,
/// or neither, by analyzing which columnrefs are used in the expressions.
fn determine_join_cond_dep(
    children: &[ArcDfPredNode],
    left_schema_size: usize,
    right_schema_size: usize,
) -> JoinCondDependency {
    let mut left_col = false;
    let mut right_col = false;
    for child in children {
        if let Some(col_ref) = ColumnRefPred::from_pred_node(child.clone()) {
            let index = col_ref.index();
            if index < left_schema_size {
                left_col = true;
            } else if index >= left_schema_size && index < left_schema_size + right_schema_size {
                right_col = true;
            } else {
                panic!(
                    "Column index {index} out of bounds {left_schema_size} + {right_schema_size}"
                );
            }
        }
    }
    match (left_col, right_col) {
        (true, true) => JoinCondDependency::Both,
        (true, false) => JoinCondDependency::Left,
        (false, true) => JoinCondDependency::Right,
        (false, false) => JoinCondDependency::None,
    }
}

/// This function recurses/loops to the bottom-level of the expression tree,
///     building a list of bottom-level exprs for each separable expr
///
/// # Arguments
/// * `categorization_fn` - Function, called with a list of each bottom-level expression, along with
///   the top-level expression node that will be categorized.
/// * `cond` - The top-level expression node to begin separating
fn categorize_conds(
    mut categorization_fn: impl FnMut(ArcDfPredNode, &[ArcDfPredNode]),
    cond: ArcDfPredNode,
) {
    fn categorize_conds_helper(
        cond: ArcDfPredNode,
        bottom_level_children: &mut Vec<ArcDfPredNode>,
    ) {
        match cond.typ {
            DfPredType::ColumnRef | DfPredType::Constant(_) => bottom_level_children.push(cond),
            _ => {
                for child in &cond.children {
                    if child.typ == DfPredType::List {
                        // TODO: What should we do when we encounter a List?
                        continue;
                    }
                    categorize_conds_helper(child.clone(), bottom_level_children);
                }
            }
        }
    }

    let mut categorize_indep_expr = |cond: ArcDfPredNode| {
        let bottom_level_children = &mut vec![];
        categorize_conds_helper(cond.clone(), bottom_level_children);
        categorization_fn(cond, bottom_level_children);
    };
    match cond.typ {
        DfPredType::LogOp(LogOpType::And) => {
            for child in &cond.children {
                categorize_indep_expr(child.clone());
            }
        }
        _ => {
            categorize_indep_expr(cond);
        }
    }
}

define_rule!(
    FilterMergeRule,
    apply_filter_merge,
    (Filter, (Filter, child))
);

fn apply_filter_merge(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let filter2 = LogicalFilter::from_plan_node(filter.child().unwrap_plan_node()).unwrap();
    let curr_cond = filter.cond();
    let child_cond = filter2.cond();
    let child = filter2.child();

    let merged_cond = merge_conds(curr_cond, child_cond);

    let new_filter = LogicalFilter::new_unchecked(child, merged_cond);
    vec![new_filter.into_plan_node().into()]
}

define_rule!(
    FilterInnerJoinTransposeRule,
    apply_filter_inner_join_transpose,
    (Filter, (Join(JoinType::Inner), child_a, child_b))
);

fn apply_filter_inner_join_transpose(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    filter_join_transpose(optimizer, binding)
}

/// Cases:
/// - Push down to the left child (only involves keys from the left child)
/// - Push down to the right child (only involves keys from the right child)
/// - Push into the join condition (involves keys from both children)
///
/// We will consider each part of the conjunction separately, and push down
/// only the relevant parts.
fn filter_join_transpose(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let filter_cond = filter.cond();
    let join = LogicalJoin::from_plan_node(filter.child().unwrap_plan_node()).unwrap();
    let join_child_a = join.left();
    let join_child_b = join.right();
    let join_cond = join.cond();
    let join_typ = join.join_type();

    let left_schema_size = optimizer.get_schema_of(join_child_a.clone()).len();
    let right_schema_size = optimizer.get_schema_of(join_child_b.clone()).len();

    let mut left_conds = vec![];
    let mut right_conds = vec![];
    let mut join_conds = vec![];
    let mut keep_conds = vec![];

    let categorization_fn = |expr: ArcDfPredNode, children: &[ArcDfPredNode]| {
        let location = determine_join_cond_dep(children, left_schema_size, right_schema_size);
        match location {
            JoinCondDependency::Left => left_conds.push(expr),
            JoinCondDependency::Right => right_conds.push(
                expr.rewrite_column_refs(|idx| {
                    Some(LogicalJoin::map_through_join(
                        idx,
                        left_schema_size,
                        right_schema_size,
                    ))
                })
                .unwrap(),
            ),
            JoinCondDependency::Both => join_conds.push(expr),
            JoinCondDependency::None => keep_conds.push(expr),
        }
    };
    categorize_conds(categorization_fn, filter_cond);

    let new_left = if !left_conds.is_empty() {
        let new_filter_node =
            LogicalFilter::new_unchecked(join_child_a, and_expr_list_to_expr(left_conds));
        PlanNodeOrGroup::PlanNode(new_filter_node.into_plan_node())
    } else {
        join_child_a
    };

    let new_right = if !right_conds.is_empty() {
        let new_filter_node =
            LogicalFilter::new_unchecked(join_child_b, and_expr_list_to_expr(right_conds));
        PlanNodeOrGroup::PlanNode(new_filter_node.into_plan_node())
    } else {
        join_child_b
    };

    let new_join = match join_typ {
        JoinType::Inner => {
            let old_cond = join_cond;
            let new_conds = merge_conds(and_expr_list_to_expr(join_conds), old_cond);
            LogicalJoin::new_unchecked(new_left, new_right, new_conds, JoinType::Inner)
        }
        _ => {
            // We don't support modifying the join condition for other join types yet
            LogicalJoin::new_unchecked(new_left, new_right, join_cond, *join_typ)
        }
    };

    let new_filter = if !keep_conds.is_empty() {
        let new_filter_node =
            LogicalFilter::new(new_join.into_plan_node(), and_expr_list_to_expr(keep_conds));
        new_filter_node.into_plan_node().into()
    } else {
        new_join.into_plan_node().into()
    };

    vec![new_filter]
}

define_rule!(
    FilterSortTransposeRule,
    apply_filter_sort_transpose,
    (Filter, (Sort, child))
);

/// Filter and sort should always be commutable.
fn apply_filter_sort_transpose(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let sort = LogicalSort::from_plan_node(filter.child().unwrap_plan_node()).unwrap();
    let child = sort.child();
    let exprs = sort.exprs();
    let cond = filter.cond();
    let new_filter_node = LogicalFilter::new_unchecked(child, cond);
    // Exprs should be the same, no projections have occurred here.
    let new_sort = LogicalSort::new(new_filter_node.into_plan_node(), exprs);
    vec![new_sort.into_plan_node().into()]
}

define_rule!(
    FilterAggTransposeRule,
    apply_filter_agg_transpose,
    (Filter, (Agg, child))
);

/// Filter is commutable past aggregations when the filter condition only
/// involves the group by columns. We will consider each part of the conjunction
/// separately, and push down only the relevant parts.
fn apply_filter_agg_transpose(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let agg = LogicalAgg::from_plan_node(filter.child().unwrap_plan_node()).unwrap();
    let child = agg.child();
    let exprs = agg.exprs();
    let groups = agg.groups();
    let cond = filter.cond();

    // Get top-level group-by columns. Does not cover cases where group-by exprs
    // are more complex than a top-level column reference.
    let group_cols = groups
        .to_vec()
        .into_iter()
        .filter_map(|expr| match expr.typ {
            DfPredType::ColumnRef => Some(ColumnRefPred::from_pred_node(expr).unwrap().index()),
            _ => None,
        })
        .collect::<HashSet<_>>();

    // Categorize predicates that only use our group-by columns as push-able.
    let mut keep_conds = vec![];
    let mut push_conds = vec![];

    let categorization_fn = |expr: ArcDfPredNode, children: &[ArcDfPredNode]| {
        let mut group_by_cols_only = true;
        for child in children {
            if let Some(col_ref) = ColumnRefPred::from_pred_node(child.clone()) {
                // The agg schema is (group columns) + (expr columns),
                // so if the column ref is < group_cols.len(), it is
                // a group column.
                if col_ref.index() >= group_cols.len() {
                    group_by_cols_only = false;
                    break;
                }
            }
        }
        if group_by_cols_only {
            push_conds.push(expr);
        } else {
            keep_conds.push(expr);
        }
    };
    categorize_conds(categorization_fn, cond);

    let new_child = if !push_conds.is_empty() {
        LogicalFilter::new_unchecked(
            child,
            LogOpPred::new_flattened_nested_logical(LogOpType::And, ListPred::new(push_conds))
                .into_pred_node(),
        )
        .into_plan_node()
        .into()
    } else {
        child
    };

    let new_agg = LogicalAgg::new_unchecked(new_child, exprs, groups);

    let new_filter = if !keep_conds.is_empty() {
        LogicalFilter::new(
            new_agg.into_plan_node(),
            LogOpPred::new_flattened_nested_logical(LogOpType::And, ListPred::new(keep_conds))
                .into_pred_node(),
        )
        .into_plan_node()
    } else {
        new_agg.into_plan_node()
    };

    vec![new_filter.into()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::plan_nodes::{BinOpPred, BinOpType, ConstantPred, LogicalScan};
    use crate::testing::new_test_optimizer;

    #[test]
    fn push_past_sort() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterSortTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let sort = LogicalSort::new(scan.into_plan_node(), ListPred::new(vec![]));

        let filter_expr = BinOpPred::new(
            ColumnRefPred::new(0).into_pred_node(),
            ConstantPred::int32(5).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        let filter = LogicalFilter::new(sort.into_plan_node(), filter_expr);

        let plan = test_optimizer.optimize(filter.into_plan_node()).unwrap();

        assert!(matches!(plan.typ, DfNodeType::Sort));
        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Filter));
    }

    #[test]
    fn filter_merge() {
        // TODO: write advanced proj with more expr that need to be transformed
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterMergeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let filter_ch_expr = BinOpPred::new(
            ColumnRefPred::new(0).into_pred_node(),
            ConstantPred::int32(1).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();
        let filter_ch = LogicalFilter::new(scan.into_plan_node(), filter_ch_expr);

        let filter_expr = BinOpPred::new(
            ColumnRefPred::new(1).into_pred_node(),
            ConstantPred::int32(6).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        let filter = LogicalFilter::new(filter_ch.into_plan_node(), filter_expr);

        let plan = test_optimizer.optimize(filter.into_plan_node()).unwrap();

        assert!(matches!(plan.typ, DfNodeType::Filter));
        let cond_log_op =
            LogOpPred::from_pred_node(LogicalFilter::from_plan_node(plan.clone()).unwrap().cond())
                .unwrap();
        assert!(matches!(cond_log_op.op_type(), LogOpType::And));

        let cond_exprs = cond_log_op.children();
        assert_eq!(cond_exprs.len(), 2);
        let expr_1 = BinOpPred::from_pred_node(cond_exprs[0].clone()).unwrap();
        let expr_2 = BinOpPred::from_pred_node(cond_exprs[1].clone()).unwrap();
        assert!(matches!(expr_1.op_type(), BinOpType::Eq));
        assert!(matches!(expr_2.op_type(), BinOpType::Eq));
        let col_1 = ColumnRefPred::from_pred_node(expr_1.left_child()).unwrap();
        let col_2 = ConstantPred::from_pred_node(expr_1.right_child()).unwrap();
        assert_eq!(col_1.index(), 1);
        assert_eq!(col_2.value().as_i32(), 6);
        let col_3 = ColumnRefPred::from_pred_node(expr_2.left_child()).unwrap();
        let col_4 = ConstantPred::from_pred_node(expr_2.right_child()).unwrap();
        assert_eq!(col_3.index(), 0);
        assert_eq!(col_4.value().as_i32(), 1);
    }

    #[test]
    fn push_past_join_conjunction() {
        // Test pushing a complex filter past a join, where one clause can
        // be pushed to the left child, one to the right child, one gets incorporated
        // into the join condition, and a constant one remains in the
        // original filter.
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterInnerJoinTransposeRule::new()));

        let scan1 = LogicalScan::new("customer".into());

        let scan2 = LogicalScan::new("orders".into());

        let join = LogicalJoin::new(
            scan1.into_plan_node(),
            scan2.into_plan_node(),
            LogOpPred::new(
                LogOpType::And,
                vec![BinOpPred::new(
                    ColumnRefPred::new(0).into_pred_node(),
                    ConstantPred::int32(1).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node()],
            )
            .into_pred_node(),
            super::JoinType::Inner,
        );

        let filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    // This one should be pushed to the left child
                    ColumnRefPred::new(0).into_pred_node(),
                    ConstantPred::int32(5).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    // This one should be pushed to the right child
                    ColumnRefPred::new(11).into_pred_node(),
                    ConstantPred::int32(6).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    // This one should be pushed to the join condition
                    ColumnRefPred::new(2).into_pred_node(),
                    ColumnRefPred::new(8).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    // always true, should be removed by other rules
                    ConstantPred::int32(2).into_pred_node(),
                    ConstantPred::int32(7).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        );

        let filter = LogicalFilter::new(join.into_plan_node(), filter_expr.into_pred_node());

        let plan = test_optimizer.optimize(filter.into_plan_node()).unwrap();

        // Examine original filter + condition
        let top_level_filter = LogicalFilter::from_plan_node(plan.clone()).unwrap();
        let bin_op_0 = BinOpPred::from_pred_node(top_level_filter.cond()).unwrap();
        assert!(matches!(bin_op_0.op_type(), BinOpType::Eq));
        let col_0 = ConstantPred::from_pred_node(bin_op_0.left_child()).unwrap();
        let col_1 = ConstantPred::from_pred_node(bin_op_0.right_child()).unwrap();
        assert_eq!(col_0.value().as_i32(), 2);
        assert_eq!(col_1.value().as_i32(), 7);

        // Examine join node + condition
        let join_node =
            LogicalJoin::from_plan_node(top_level_filter.child().unwrap_plan_node()).unwrap();
        let join_conds = LogOpPred::from_pred_node(join_node.cond()).unwrap();
        assert!(matches!(join_conds.op_type(), LogOpType::And));
        assert_eq!(join_conds.children().len(), 2);
        let bin_op_1 = BinOpPred::from_pred_node(join_conds.children()[0].clone()).unwrap();
        assert!(matches!(bin_op_1.op_type(), BinOpType::Eq));
        let col_2 = ColumnRefPred::from_pred_node(bin_op_1.left_child()).unwrap();
        let col_3 = ColumnRefPred::from_pred_node(bin_op_1.right_child()).unwrap();
        assert_eq!(col_2.index(), 2);
        assert_eq!(col_3.index(), 8);

        // Examine left child filter + condition
        let filter_1 = LogicalFilter::from_plan_node(join_node.left().unwrap_plan_node()).unwrap();
        let bin_op_3 = BinOpPred::from_pred_node(filter_1.cond()).unwrap();
        assert!(matches!(bin_op_3.op_type(), BinOpType::Eq));
        let col_6 = ColumnRefPred::from_pred_node(bin_op_3.left_child()).unwrap();
        let col_7 = ConstantPred::from_pred_node(bin_op_3.right_child()).unwrap();
        assert_eq!(col_6.index(), 0);
        assert_eq!(col_7.value().as_i32(), 5);

        // Examine right child filter + condition
        let filter_2 = LogicalFilter::from_plan_node(join_node.right().unwrap_plan_node()).unwrap();
        let bin_op_4 = BinOpPred::from_pred_node(filter_2.cond()).unwrap();
        assert!(matches!(bin_op_4.op_type(), BinOpType::Eq));
        let col_8 = ColumnRefPred::from_pred_node(bin_op_4.left_child()).unwrap();
        let col_9 = ConstantPred::from_pred_node(bin_op_4.right_child()).unwrap();
        assert_eq!(col_8.index(), 3);
        assert_eq!(col_9.value().as_i32(), 6);
    }

    #[test]
    fn push_past_agg() {
        // Test pushing a filter past an aggregation node, where the filter
        // condition has one clause that can be pushed down to the child and
        // one that must remain in the filter.
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterAggTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let agg = LogicalAgg::new(
            scan.clone().into_plan_node(),
            ListPred::new(vec![]),
            ListPred::new(vec![ColumnRefPred::new(0).into_pred_node()]),
        );

        let filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    // This one should be pushed to the child
                    ColumnRefPred::new(0).into_pred_node(),
                    ConstantPred::int32(5).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    // This one should remain in the filter
                    ColumnRefPred::new(1).into_pred_node(),
                    ConstantPred::int32(6).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        );

        let filter = LogicalFilter::new(agg.into_plan_node(), filter_expr.into_pred_node());

        let plan = test_optimizer.optimize(filter.into_plan_node()).unwrap();

        let plan_filter = LogicalFilter::from_plan_node(plan.clone()).unwrap();
        let plan_filter_expr = LogOpPred::from_pred_node(plan_filter.cond()).unwrap();
        assert!(matches!(plan_filter_expr.op_type(), LogOpType::And));
        assert_eq!(plan_filter_expr.children().len(), 1);
        let op_0 = BinOpPred::from_pred_node(plan_filter_expr.children()[0].clone()).unwrap();
        let col_0 = ColumnRefPred::from_pred_node(op_0.left_child()).unwrap();
        assert_eq!(col_0.index(), 1);
        let col_1 = ConstantPred::from_pred_node(op_0.right_child()).unwrap();
        assert_eq!(col_1.value().as_i32(), 6);

        let plan_agg = LogicalAgg::from_plan_node(plan.child_rel(0)).unwrap();
        let plan_agg_groups = plan_agg.groups();
        assert_eq!(plan_agg_groups.len(), 1);
        let group_col = ColumnRefPred::from_pred_node(plan_agg_groups.child(0))
            .unwrap()
            .index();
        assert_eq!(group_col, 0);

        let plan_agg_child_filter =
            LogicalFilter::from_plan_node(plan_agg.child().unwrap_plan_node()).unwrap();
        let plan_agg_child_filter_expr =
            LogOpPred::from_pred_node(plan_agg_child_filter.cond()).unwrap();
        assert!(matches!(
            plan_agg_child_filter_expr.op_type(),
            LogOpType::And
        ));
        assert_eq!(plan_agg_child_filter_expr.children().len(), 1);
        let op_1 = BinOpPred::from_pred_node(plan_agg_child_filter_expr.child(0)).unwrap();
        let col_2 = ColumnRefPred::from_pred_node(op_1.left_child()).unwrap();
        assert_eq!(col_2.index(), 0);
        let col_3 = ConstantPred::from_pred_node(op_1.right_child()).unwrap();
        assert_eq!(col_3.value().as_i32(), 5);
    }
}
