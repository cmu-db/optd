// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Memo table extensions

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use optd_core::cascades::{ExprId, GroupId, Memo};
use optd_core::nodes::NodeType;

use crate::plan_nodes::{ConstantPred, DfNodeType, DfReprPredNode};

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum LogicalJoinOrder {
    Table(Arc<str>),
    Join(Box<Self>, Box<Self>),
}

impl std::fmt::Display for LogicalJoinOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalJoinOrder::Table(name) => write!(f, "{}", name),
            LogicalJoinOrder::Join(left, right) => {
                write!(f, "(Join {} {})", left, right)
            }
        }
    }
}

pub trait MemoExt {
    fn enumerate_join_order(&self, entry: GroupId) -> Vec<LogicalJoinOrder>;
}

fn enumerate_join_order_expr_inner<M: Memo<DfNodeType> + ?Sized>(
    memo: &M,
    current: ExprId,
    visited: &mut HashMap<GroupId, Arc<[LogicalJoinOrder]>>,
    warning_fired: &mut bool,
) -> Vec<LogicalJoinOrder> {
    let expr = memo.get_expr_memoed(current);
    match &expr.typ {
        DfNodeType::Scan => {
            let table = memo.get_pred(expr.predicates[0]); // TODO: use unified repr
            let table = ConstantPred::from_pred_node(table)
                .unwrap()
                .value()
                .as_str();
            vec![LogicalJoinOrder::Table(table)]
        }
        DfNodeType::Join(_) | DfNodeType::DepJoin | DfNodeType::RawDepJoin(_) => {
            // Assume child 0 == left, child 1 == right
            let left = expr.children[0];
            let right = expr.children[1];
            let left_join_orders =
                enumerate_join_order_group_inner(memo, left, visited, warning_fired);
            let right_join_orders =
                enumerate_join_order_group_inner(memo, right, visited, warning_fired);
            let mut join_orders = BTreeSet::new();
            for left_join_order in left_join_orders.iter() {
                for right_join_order in right_join_orders.iter() {
                    join_orders.insert(LogicalJoinOrder::Join(
                        Box::new(left_join_order.clone()),
                        Box::new(right_join_order.clone()),
                    ));
                }
            }
            join_orders.into_iter().collect()
        }
        typ if typ.is_logical() => {
            const MAX_JOIN_ORDER_OUTPUT: usize = 20;
            let mut join_orders = BTreeSet::new();
            'outer: for (idx, child) in expr.children.iter().enumerate() {
                let child_join_orders =
                    enumerate_join_order_group_inner(memo, *child, visited, warning_fired);
                if idx == 0 {
                    for child_join_order in child_join_orders.iter() {
                        join_orders.insert(child_join_order.clone());
                        if join_orders.len() > MAX_JOIN_ORDER_OUTPUT && !*warning_fired {
                            *warning_fired = true;
                            tracing::warn!(
                                "too many join orders, returning the first {} items, TODO: only enumerate join orders when requested",
                                MAX_JOIN_ORDER_OUTPUT
                            );
                            break 'outer;
                        }
                    }
                } else {
                    assert!(
                        child_join_orders.is_empty(),
                        "missing join node? found a node with join orders on multiple children"
                    );
                }
            }

            join_orders
                .iter()
                .take(MAX_JOIN_ORDER_OUTPUT)
                .map(|x| (*x).clone())
                .collect_vec()
        }
        _ => Vec::new(),
    }
}

fn enumerate_join_order_group_inner<M: Memo<DfNodeType> + ?Sized>(
    memo: &M,
    current: GroupId,
    visited: &mut HashMap<GroupId, Arc<[LogicalJoinOrder]>>,
    warning_fired: &mut bool,
) -> Arc<[LogicalJoinOrder]> {
    if let Some(result) = visited.get(&current) {
        return result.clone();
    }
    // If the current node is processed again before the result gets populated, simply return an
    // empty list, as another search path will eventually return a correct for it, and then get
    // combined with this empty list.
    visited.insert(current, Arc::new([]));
    let group_exprs = memo.get_all_exprs_in_group(current);
    let mut join_orders = BTreeSet::new();
    for expr_id in group_exprs {
        let expr_join_orders =
            enumerate_join_order_expr_inner(memo, expr_id, visited, warning_fired);
        for expr_join_order in expr_join_orders {
            join_orders.insert(expr_join_order);
        }
    }
    let res: Arc<[_]> = join_orders.into_iter().collect_vec().into();
    visited.insert(current, res.clone());
    res
}

impl<M: Memo<DfNodeType>> MemoExt for M {
    fn enumerate_join_order(&self, entry: GroupId) -> Vec<LogicalJoinOrder> {
        let mut visited = HashMap::new();
        enumerate_join_order_group_inner(self, entry, &mut visited, &mut false)
            .iter()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use optd_core::cascades::NaiveMemo;
    use optd_core::nodes::{PlanNodeOrGroup, Value};

    use super::*;
    use crate::plan_nodes::{
        ConstantPred, DfReprPlanNode, DfReprPredNode, JoinType, ListPred, LogicalJoin,
        LogicalProjection, LogicalScan,
    };

    #[test]
    fn enumerate_join_orders() {
        let mut memo = NaiveMemo::<DfNodeType>::new(Arc::new([]));
        let (group, _) = memo.add_new_expr(
            LogicalJoin::new(
                LogicalScan::new("t1".to_string()).into_plan_node(),
                LogicalScan::new("t2".to_string()).into_plan_node(),
                ConstantPred::new(Value::Bool(true)).into_pred_node(),
                JoinType::Inner,
            )
            .into_plan_node(),
        );
        // Add an alternative join order
        memo.add_expr_to_group(
            LogicalProjection::new(
                LogicalJoin::new(
                    LogicalScan::new("t2".to_string()).into_plan_node(),
                    LogicalScan::new("t1".to_string()).into_plan_node(),
                    ConstantPred::new(Value::Bool(true)).into_pred_node(),
                    JoinType::Inner,
                )
                .into_plan_node(),
                ListPred::new(Vec::new()),
            )
            .into_plan_node()
            .into(),
            group,
        );
        // Self-reference group
        memo.add_expr_to_group(
            LogicalProjection::new_unchecked(
                PlanNodeOrGroup::Group(group),
                ListPred::new(Vec::new()),
            )
            .into_plan_node()
            .into(),
            group,
        );
        let orders = memo.enumerate_join_order(group);
        assert_eq!(
            orders,
            vec![
                LogicalJoinOrder::Join(
                    Box::new(LogicalJoinOrder::Table("t1".into())),
                    Box::new(LogicalJoinOrder::Table("t2".into())),
                ),
                LogicalJoinOrder::Join(
                    Box::new(LogicalJoinOrder::Table("t2".into())),
                    Box::new(LogicalJoinOrder::Table("t1".into())),
                )
            ]
        );
    }
}
