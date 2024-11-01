//! This rule is designed to be applied heuristically (read: all the time, blindly).
//! However, pushing a filter is not *always* better (but it usually is). If cost is
//! to be taken into account, each transposition step can be done separately
//! (and are thus all in independent functions).
//! One can even implement each of these helper functions as their own transpose rule,
//! like Calcite does.
//!
//! At a high level, filter pushdown is responsible for pushing the filter node
//! further down the query plan whenever it is possible to do so.

use core::panic;
use std::collections::{HashMap, HashSet};
use std::vec;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    ColumnRefExpr, Expr, ExprList, JoinType, LogOpExpr, LogOpType, LogicalAgg, LogicalFilter,
    LogicalJoin, LogicalSort, OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;

use super::filter::simplify_log_expr;
use super::macros::define_rule;

/// Emits a LogOpExpr AND if the list has more than one element
/// Otherwise, returns the single element
fn and_expr_list_to_expr(exprs: Vec<Expr>) -> Expr {
    if exprs.len() == 1 {
        exprs.first().unwrap().clone()
    } else {
        LogOpExpr::new(LogOpType::And, ExprList::new(exprs)).into_expr()
    }
}

fn merge_conds(first: Expr, second: Expr) -> Expr {
    let new_expr_list = ExprList::new(vec![first, second]);
    // Flatten nested logical expressions if possible
    let flattened =
        LogOpExpr::new_flattened_nested_logical(LogOpType::And, new_expr_list).into_expr();
    let mut changed = false;
    // TODO: such simplifications should be invoked from optd-core, instead of ad-hoc
    Expr::from_rel_node(simplify_log_expr(flattened.into_rel_node(), &mut changed)).unwrap()
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
    children: &Vec<Expr>,
    left_schema_size: usize,
    right_schema_size: usize,
) -> JoinCondDependency {
    let mut left_col = false;
    let mut right_col = false;
    for child in children {
        if child.typ() == OptRelNodeTyp::ColumnRef {
            let col_ref = ColumnRefExpr::from_rel_node(child.clone().into_rel_node()).unwrap();
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
/// * `categorization_fn` - Function, called with a list of each bottom-level
///     expression, along with the top-level expression node that will be
///     categorized.
/// * `cond` - The top-level expression node to begin separating
fn categorize_conds(mut categorization_fn: impl FnMut(Expr, &Vec<Expr>), cond: Expr) {
    fn categorize_conds_helper(cond: Expr, bottom_level_children: &mut Vec<Expr>) {
        assert!(cond.typ().is_expression());
        match cond.typ() {
            OptRelNodeTyp::ColumnRef | OptRelNodeTyp::Constant(_) => {
                bottom_level_children.push(cond)
            }
            _ => {
                for child in &cond.clone().into_rel_node().children {
                    if child.typ == OptRelNodeTyp::List {
                        // TODO: What should we do when we encounter a List?
                        continue;
                    }
                    categorize_conds_helper(
                        Expr::from_rel_node(child.clone()).unwrap(),
                        bottom_level_children,
                    );
                }
            }
        }
    }

    let mut categorize_indep_expr = |cond: Expr| {
        let bottom_level_children = &mut vec![];
        categorize_conds_helper(cond.clone(), bottom_level_children);
        categorization_fn(cond, bottom_level_children);
    };
    match cond.typ() {
        OptRelNodeTyp::LogOp(LogOpType::And) => {
            for child in &cond.into_rel_node().children {
                categorize_indep_expr(Expr::from_rel_node(child.clone()).unwrap());
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
    (Filter, (Filter, child, [cond1]), [cond])
);

fn apply_filter_merge(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterMergeRulePicks { child, cond1, cond }: FilterMergeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let curr_cond = Expr::from_rel_node(cond.into()).unwrap();
    let child_cond = Expr::from_rel_node(cond1.into()).unwrap();

    let merged_cond = merge_conds(curr_cond, child_cond);

    let new_filter = LogicalFilter::new(child, merged_cond);
    vec![new_filter.into_rel_node().as_ref().clone()]
}

// TODO: define_rule! should be able to match on any join type, ideally...
define_rule!(
    FilterCrossJoinTransposeRule,
    apply_filter_cross_join_transpose,
    (
        Filter,
        (Join(JoinType::Cross), child_a, child_b, [join_cond]),
        [cond]
    )
);

fn apply_filter_cross_join_transpose(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterCrossJoinTransposeRulePicks {
        child_a,
        child_b,
        join_cond,
        cond,
    }: FilterCrossJoinTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    filter_join_transpose(
        optimizer,
        JoinType::Cross,
        child_a,
        child_b,
        join_cond,
        cond,
    )
}

define_rule!(
    FilterInnerJoinTransposeRule,
    apply_filter_inner_join_transpose,
    (
        Filter,
        (Join(JoinType::Inner), child_a, child_b, [join_cond]),
        [cond]
    )
);

fn apply_filter_inner_join_transpose(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterInnerJoinTransposeRulePicks {
        child_a,
        child_b,
        join_cond,
        cond,
    }: FilterInnerJoinTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    filter_join_transpose(
        optimizer,
        JoinType::Inner,
        child_a,
        child_b,
        join_cond,
        cond,
    )
}

/// Cases:
/// - Push down to the left child (only involves keys from the left child)
/// - Push down to the right child (only involves keys from the right child)
/// - Push into the join condition (involves keys from both children)
/// We will consider each part of the conjunction separately, and push down
/// only the relevant parts.
fn filter_join_transpose(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    join_typ: JoinType,
    join_child_a: RelNode<OptRelNodeTyp>,
    join_child_b: RelNode<OptRelNodeTyp>,
    join_cond: RelNode<OptRelNodeTyp>,
    filter_cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let left_schema_size = optimizer
        .get_property::<SchemaPropertyBuilder>(join_child_a.clone().into(), 0)
        .len();
    let right_schema_size = optimizer
        .get_property::<SchemaPropertyBuilder>(join_child_b.clone().into(), 0)
        .len();

    let join_child_a = PlanNode::from_group(join_child_a.into());
    let join_child_b = PlanNode::from_group(join_child_b.into());
    let join_cond = Expr::from_rel_node(join_cond.into()).unwrap();
    let filter_cond = Expr::from_rel_node(filter_cond.into()).unwrap();
    // TODO: Push existing join conditions down as well

    let mut left_conds = vec![];
    let mut right_conds = vec![];
    let mut join_conds = vec![];
    let mut keep_conds = vec![];

    let categorization_fn = |expr: Expr, children: &Vec<Expr>| {
        let location = determine_join_cond_dep(children, left_schema_size, right_schema_size);
        match location {
            JoinCondDependency::Left => left_conds.push(expr),
            JoinCondDependency::Right => right_conds.push(
                expr.rewrite_column_refs(&mut |idx| {
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
        let new_filter_node = LogicalFilter::new(join_child_a, and_expr_list_to_expr(left_conds));
        new_filter_node.into_plan_node()
    } else {
        join_child_a
    };

    let new_right = if !right_conds.is_empty() {
        let new_filter_node = LogicalFilter::new(join_child_b, and_expr_list_to_expr(right_conds));
        new_filter_node.into_plan_node()
    } else {
        join_child_b
    };

    let new_join = match join_typ {
        JoinType::Inner => {
            let old_cond = join_cond;
            let new_conds = merge_conds(and_expr_list_to_expr(join_conds), old_cond);
            LogicalJoin::new(new_left, new_right, new_conds, JoinType::Inner)
        }
        JoinType::Cross => {
            if !join_conds.is_empty() {
                LogicalJoin::new(
                    new_left,
                    new_right,
                    and_expr_list_to_expr(join_conds),
                    JoinType::Inner,
                )
            } else {
                LogicalJoin::new(new_left, new_right, join_cond, JoinType::Cross)
            }
        }
        _ => {
            // We don't support modifying the join condition for other join types yet
            LogicalJoin::new(new_left, new_right, join_cond, join_typ)
        }
    };

    let new_filter = if !keep_conds.is_empty() {
        let new_filter_node =
            LogicalFilter::new(new_join.into_plan_node(), and_expr_list_to_expr(keep_conds));
        new_filter_node.into_rel_node().as_ref().clone()
    } else {
        new_join.into_rel_node().as_ref().clone()
    };

    vec![new_filter]
}

define_rule!(
    FilterSortTransposeRule,
    apply_filter_sort_transpose,
    (Filter, (Sort, child, [exprs]), [cond])
);

/// Filter and sort should always be commutable.
fn apply_filter_sort_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterSortTransposeRulePicks { child, exprs, cond }: FilterSortTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();

    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let new_filter_node = LogicalFilter::new(child, cond_as_expr);
    // Exprs should be the same, no projections have occurred here.
    let new_sort = LogicalSort::new(new_filter_node.into_plan_node(), exprs);
    vec![new_sort.into_rel_node().as_ref().clone()]
}

define_rule!(
    FilterAggTransposeRule,
    apply_filter_agg_transpose,
    (Filter, (Agg, child, [exprs], [groups]), [cond])
);

/// Filter is commutable past aggregations when the filter condition only
/// involves the group by columns. We will consider each part of the conjunction
/// separately, and push down only the relevant parts.
fn apply_filter_agg_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterAggTransposeRulePicks {
        child,
        exprs,
        groups,
        cond,
    }: FilterAggTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();
    let groups = ExprList::from_rel_node(groups.into()).unwrap();
    let child = PlanNode::from_group(child.into());

    // Get top-level group-by columns. Does not cover cases where group-by exprs
    // are more complex than a top-level column reference.
    let group_cols = groups
        .clone()
        .into_rel_node()
        .children
        .iter()
        .filter_map(|expr| match expr.typ {
            OptRelNodeTyp::ColumnRef => {
                Some(ColumnRefExpr::from_rel_node(expr.clone()).unwrap().index())
            }
            _ => None,
        })
        .collect::<HashSet<_>>();

    // Categorize predicates that only use our group-by columns as push-able.
    let mut keep_conds = vec![];
    let mut push_conds = vec![];

    let categorization_fn = |expr: Expr, children: &Vec<Expr>| {
        let mut group_by_cols_only = true;
        for child in children {
            if child.typ() == OptRelNodeTyp::ColumnRef {
                let col_ref = ColumnRefExpr::from_rel_node(child.clone().into_rel_node()).unwrap();
                if !group_cols.contains(&col_ref.index()) {
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
    categorize_conds(categorization_fn, Expr::from_rel_node(cond.into()).unwrap());

    let new_child = if !push_conds.is_empty() {
        LogicalFilter::new(
            child,
            LogOpExpr::new_flattened_nested_logical(LogOpType::And, ExprList::new(push_conds))
                .into_expr(),
        )
        .into_plan_node()
    } else {
        child
    };

    let new_agg = LogicalAgg::new(new_child, exprs, groups);

    let new_filter = if !keep_conds.is_empty() {
        LogicalFilter::new(
            new_agg.into_plan_node(),
            LogOpExpr::new_flattened_nested_logical(LogOpType::And, ExprList::new(keep_conds))
                .into_expr(),
        )
        .into_rel_node()
        .as_ref()
        .clone()
    } else {
        new_agg.into_rel_node().as_ref().clone()
    };

    vec![new_filter]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimizer::Optimizer;

    use crate::{
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ExprList, LogOpExpr, LogOpType,
            LogicalAgg, LogicalFilter, LogicalJoin, LogicalScan, LogicalSort, OptRelNode,
            OptRelNodeTyp,
        },
        rules::{
            FilterAggTransposeRule, FilterInnerJoinTransposeRule, FilterMergeRule,
            FilterSortTransposeRule,
        },
        testing::new_test_optimizer,
    };

    #[test]
    fn push_past_sort() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterSortTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let sort = LogicalSort::new(scan.into_plan_node(), ExprList::new(vec![]));

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let filter = LogicalFilter::new(sort.into_plan_node(), filter_expr);

        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Sort));
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
    }

    #[test]
    fn filter_merge() {
        // TODO: write advanced proj with more expr that need to be transformed
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterMergeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let filter_ch_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(1).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();
        let filter_ch = LogicalFilter::new(scan.into_plan_node(), filter_ch_expr);

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(1).into_expr(),
            ConstantExpr::int32(6).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let filter = LogicalFilter::new(filter_ch.into_plan_node(), filter_expr);

        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Filter));
        let cond_log_op = LogOpExpr::from_rel_node(
            LogicalFilter::from_rel_node(plan.clone())
                .unwrap()
                .cond()
                .into_rel_node(),
        )
        .unwrap();
        assert!(matches!(cond_log_op.op_type(), LogOpType::And));

        let cond_exprs = cond_log_op.children();
        assert_eq!(cond_exprs.len(), 2);
        let expr_1 = BinOpExpr::from_rel_node(cond_exprs[0].clone().into_rel_node()).unwrap();
        let expr_2 = BinOpExpr::from_rel_node(cond_exprs[1].clone().into_rel_node()).unwrap();
        assert!(matches!(expr_1.op_type(), BinOpType::Eq));
        assert!(matches!(expr_2.op_type(), BinOpType::Eq));
        let col_1 =
            ColumnRefExpr::from_rel_node(expr_1.left_child().clone().into_rel_node()).unwrap();
        let col_2 =
            ConstantExpr::from_rel_node(expr_1.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_1.index(), 1);
        assert_eq!(col_2.value().as_i32(), 6);
        let col_3 =
            ColumnRefExpr::from_rel_node(expr_2.left_child().clone().into_rel_node()).unwrap();
        let col_4 =
            ConstantExpr::from_rel_node(expr_2.right_child().clone().into_rel_node()).unwrap();
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
            LogOpExpr::new(
                LogOpType::And,
                ExprList::new(vec![BinOpExpr::new(
                    ColumnRefExpr::new(0).into_expr(),
                    ConstantExpr::int32(1).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr()]),
            )
            .into_expr(),
            super::JoinType::Inner,
        );

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    // This one should be pushed to the left child
                    ColumnRefExpr::new(0).into_expr(),
                    ConstantExpr::int32(5).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the right child
                    ColumnRefExpr::new(11).into_expr(),
                    ConstantExpr::int32(6).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the join condition
                    ColumnRefExpr::new(2).into_expr(),
                    ColumnRefExpr::new(8).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // always true, should be removed by other rules
                    ConstantExpr::int32(2).into_expr(),
                    ConstantExpr::int32(7).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        );

        let filter = LogicalFilter::new(join.into_plan_node(), filter_expr.into_expr());

        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        // Examine original filter + condition
        let top_level_filter = LogicalFilter::from_rel_node(plan.clone()).unwrap();
        let bin_op_0 =
            BinOpExpr::from_rel_node(top_level_filter.cond().clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_0.op_type(), BinOpType::Eq));
        let col_0 =
            ConstantExpr::from_rel_node(bin_op_0.left_child().clone().into_rel_node()).unwrap();
        let col_1 =
            ConstantExpr::from_rel_node(bin_op_0.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_0.value().as_i32(), 2);
        assert_eq!(col_1.value().as_i32(), 7);

        // Examine join node + condition
        let join_node =
            LogicalJoin::from_rel_node(top_level_filter.child().clone().into_rel_node()).unwrap();
        let join_conds = LogOpExpr::from_rel_node(join_node.cond().into_rel_node()).unwrap();
        assert!(matches!(join_conds.op_type(), LogOpType::And));
        assert_eq!(join_conds.children().len(), 2);
        let bin_op_1 =
            BinOpExpr::from_rel_node(join_conds.children()[0].clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_1.op_type(), BinOpType::Eq));
        let col_2 =
            ColumnRefExpr::from_rel_node(bin_op_1.left_child().clone().into_rel_node()).unwrap();
        let col_3 =
            ColumnRefExpr::from_rel_node(bin_op_1.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_2.index(), 2);
        assert_eq!(col_3.index(), 8);

        // Examine left child filter + condition
        let filter_1 = LogicalFilter::from_rel_node(join_node.left().into_rel_node()).unwrap();
        let bin_op_3 = BinOpExpr::from_rel_node(filter_1.cond().clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_3.op_type(), BinOpType::Eq));
        let col_6 =
            ColumnRefExpr::from_rel_node(bin_op_3.left_child().clone().into_rel_node()).unwrap();
        let col_7 =
            ConstantExpr::from_rel_node(bin_op_3.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_6.index(), 0);
        assert_eq!(col_7.value().as_i32(), 5);

        // Examine right child filter + condition
        let filter_2 = LogicalFilter::from_rel_node(join_node.right().into_rel_node()).unwrap();
        let bin_op_4 = BinOpExpr::from_rel_node(filter_2.cond().clone().into_rel_node()).unwrap();
        assert!(matches!(bin_op_4.op_type(), BinOpType::Eq));
        let col_8 =
            ColumnRefExpr::from_rel_node(bin_op_4.left_child().clone().into_rel_node()).unwrap();
        let col_9 =
            ConstantExpr::from_rel_node(bin_op_4.right_child().clone().into_rel_node()).unwrap();
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
            ExprList::new(vec![]),
            ExprList::new(vec![ColumnRefExpr::new(0).into_expr()]),
        );

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    // This one should be pushed to the child
                    ColumnRefExpr::new(0).into_expr(),
                    ConstantExpr::int32(5).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should remain in the filter
                    ColumnRefExpr::new(1).into_expr(),
                    ConstantExpr::int32(6).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        );

        let filter = LogicalFilter::new(agg.into_plan_node(), filter_expr.into_expr());

        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        let plan_filter = LogicalFilter::from_rel_node(plan.clone()).unwrap();
        assert!(matches!(plan_filter.0.typ(), OptRelNodeTyp::Filter));
        let plan_filter_expr =
            LogOpExpr::from_rel_node(plan_filter.cond().into_rel_node()).unwrap();
        assert!(matches!(plan_filter_expr.op_type(), LogOpType::And));
        assert_eq!(plan_filter_expr.children().len(), 1);
        let op_0 = BinOpExpr::from_rel_node(plan_filter_expr.children()[0].clone().into_rel_node())
            .unwrap();
        let col_0 =
            ColumnRefExpr::from_rel_node(op_0.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_0.index(), 1);
        let col_1 =
            ConstantExpr::from_rel_node(op_0.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_1.value().as_i32(), 6);

        let plan_agg = LogicalAgg::from_rel_node(plan.child(0)).unwrap();
        let plan_agg_groups = plan_agg.groups();
        assert_eq!(plan_agg_groups.len(), 1);
        let group_col = ColumnRefExpr::from_rel_node(plan_agg_groups.child(0).into_rel_node())
            .unwrap()
            .index();
        assert_eq!(group_col, 0);

        let plan_agg_child_filter =
            LogicalFilter::from_rel_node(plan_agg.child().into_rel_node()).unwrap();
        let plan_agg_child_filter_expr =
            LogOpExpr::from_rel_node(plan_agg_child_filter.cond().into_rel_node()).unwrap();
        assert!(matches!(
            plan_agg_child_filter_expr.op_type(),
            LogOpType::And
        ));
        assert_eq!(plan_agg_child_filter_expr.children().len(), 1);
        let op_1 =
            BinOpExpr::from_rel_node(plan_agg_child_filter_expr.child(0).into_rel_node()).unwrap();
        let col_2 =
            ColumnRefExpr::from_rel_node(op_1.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_2.index(), 0);
        let col_3 =
            ConstantExpr::from_rel_node(op_1.right_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_3.value().as_i32(), 5);
    }
}
