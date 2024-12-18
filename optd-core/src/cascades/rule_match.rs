// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use itertools::Itertools;

use crate::cascades::memo::ArcMemoPlanNode;
use crate::cascades::optimizer::{CascadesOptimizer, ExprId};
use crate::cascades::{GroupId, Memo};
use crate::nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup};
use crate::rules::RuleMatcher;

fn match_node<T: NodeType, M: Memo<T>>(
    children: &[RuleMatcher<T>],
    node: ArcMemoPlanNode<T>,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<ArcPlanNode<T>> {
    let predicates = node
        .predicates
        .iter()
        .map(|pred_id| optimizer.get_pred(*pred_id))
        .collect_vec();
    if let [RuleMatcher::AnyMany] = children {
        return vec![Arc::new(PlanNode {
            typ: node.typ.clone(),
            children: node
                .children
                .iter()
                .map(|x| PlanNodeOrGroup::Group(*x))
                .collect(),
            predicates,
        })];
    }
    assert_eq!(children.len(), node.children.len(), "mismatched matcher");
    let mut matched_children = Vec::new();
    for (idx, child) in children.iter().enumerate() {
        match child {
            RuleMatcher::Any => {
                matched_children.push(vec![PlanNodeOrGroup::Group(node.children[idx])]);
            }
            RuleMatcher::AnyMany => {
                unreachable!();
            }
            _ => {
                let child_bindings = match_and_pick_group(child, node.children[idx], optimizer);
                if child_bindings.is_empty() {
                    return vec![];
                }
                matched_children.push(
                    child_bindings
                        .into_iter()
                        .map(PlanNodeOrGroup::PlanNode)
                        .collect(),
                );
            }
        }
    }
    matched_children
        .into_iter()
        .fold(vec![vec![]], |acc, child| {
            let mut new = Vec::new();
            for a in acc {
                for c in &child {
                    let mut a = a.clone();
                    a.push(c.clone());
                    new.push(a);
                }
            }
            new
        })
        .into_iter()
        .map(|children| {
            assert!(children.len() == node.children.len());
            Arc::new(PlanNode {
                typ: node.typ.clone(),
                children,
                predicates: predicates.clone(),
            })
        })
        .collect()
}

fn match_and_pick<T: NodeType, M: Memo<T>>(
    matcher: &RuleMatcher<T>,
    node: ArcMemoPlanNode<T>,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<ArcPlanNode<T>> {
    match matcher {
        RuleMatcher::MatchNode { typ, children } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(children, node, optimizer)
        }
        RuleMatcher::MatchDiscriminant {
            typ_discriminant,
            children,
        } => {
            if &std::mem::discriminant(&node.typ) != typ_discriminant {
                return vec![];
            }
            match_node(children, node, optimizer)
        }
        _ => panic!("top node should be match node"),
    }
}

pub(crate) fn match_and_pick_expr<T: NodeType, M: Memo<T>>(
    matcher: &RuleMatcher<T>,
    expr_id: ExprId,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<ArcPlanNode<T>> {
    let node = optimizer.get_expr_memoed(expr_id);
    match_and_pick(matcher, node, optimizer)
}

fn match_and_pick_group<T: NodeType, M: Memo<T>>(
    matcher: &RuleMatcher<T>,
    group_id: GroupId,
    optimizer: &CascadesOptimizer<T, M>,
) -> Vec<ArcPlanNode<T>> {
    let mut matches = vec![];
    for expr_id in optimizer.get_all_exprs_in_group(group_id) {
        let node = optimizer.get_expr_memoed(expr_id);
        matches.extend(match_and_pick(matcher, node, optimizer));
    }
    matches
}
