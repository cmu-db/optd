use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use itertools::Itertools;
use tracing::trace;

use crate::{
    cascades::{
        memo::RelMemoNodeRef,
        optimizer::{CascadesOptimizer, ExprId, RuleId},
        tasks::{OptimizeExpressionTask, OptimizeInputsTask},
        GroupId,
    },
    rel_node::{RelNode, RelNodeTyp},
    rules::RuleMatcher,
};

use super::Task;

pub struct ApplyRuleTask {
    rule_id: RuleId,
    expr_id: ExprId,
    exploring: bool,
}

impl ApplyRuleTask {
    pub fn new(rule_id: RuleId, expr_id: ExprId, exploring: bool) -> Self {
        Self {
            rule_id,
            expr_id,
            exploring,
        }
    }
}

fn match_node<T: RelNodeTyp>(
    typ: &T,
    children: &[RuleMatcher<T>],
    pick_to: Option<usize>,
    node: RelMemoNodeRef<T>,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    if let RuleMatcher::PickMany { .. } | RuleMatcher::IgnoreMany = children.last().unwrap() {
    } else {
        assert_eq!(
            children.len(),
            node.children.len(),
            "children size unmatched, please fix the rule: {}",
            node
        );
    }

    let mut should_end = false;
    let mut picks = vec![HashMap::new()];
    for (idx, child) in children.iter().enumerate() {
        assert!(!should_end, "many matcher should be at the end");
        match child {
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                should_end = true;
            }
            RuleMatcher::PickOne { pick_to } => {
                for pick in &mut picks {
                    let res = pick.insert(*pick_to, RelNode::new_group(node.children[idx]));
                    assert!(res.is_none(), "dup pick");
                }
            }
            RuleMatcher::PickMany { pick_to } => {
                for pick in &mut picks {
                    let res = pick.insert(
                        *pick_to,
                        RelNode::new_list(
                            node.children[idx..]
                                .iter()
                                .map(|x| Arc::new(RelNode::new_group(*x)))
                                .collect_vec(),
                        ),
                    );
                    assert!(res.is_none(), "dup pick");
                }
                should_end = true;
            }
            _ => {
                let new_picks = match_and_pick_group(child, node.children[idx], optimizer);
                let mut merged_picks = vec![];
                for old_pick in &picks {
                    for new_picks in &new_picks {
                        let mut pick = old_pick.clone();
                        pick.extend(new_picks.iter().map(|(k, v)| (*k, v.clone())));
                        merged_picks.push(pick);
                    }
                }
                picks = merged_picks;
            }
        }
    }
    if let Some(pick_to) = pick_to {
        for pick in &mut picks {
            let res: Option<RelNode<T>> = pick.insert(
                pick_to,
                RelNode {
                    typ: *typ,
                    children: node
                        .children
                        .iter()
                        .map(|x| RelNode::new_group(*x).into())
                        .collect_vec(),
                    data: node.data.clone(),
                },
            );
            assert!(res.is_none(), "dup pick");
        }
    }
    picks
}

fn match_and_pick_group<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    group_id: GroupId,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    let mut matches = vec![];
    for expr_id in optimizer.get_all_exprs_in_group(group_id) {
        let node = optimizer.get_expr_memoed(expr_id);
        matches.extend(match_and_pick(matcher, node, optimizer));
    }
    matches
}

fn match_and_pick<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    node: RelMemoNodeRef<T>,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    match matcher {
        RuleMatcher::MatchAndPickNode {
            typ,
            children,
            pick_to,
        } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(typ, children, Some(*pick_to), node, optimizer)
        }
        RuleMatcher::MatchNode { typ, children } => {
            if &node.typ != typ {
                return vec![];
            }
            match_node(typ, children, None, node, optimizer)
        }
        _ => panic!("top node should be match node"),
    }
}

impl<T: RelNodeTyp> Task<T> for ApplyRuleTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        if optimizer.is_rule_fired(self.expr_id, self.rule_id) {
            return Ok(vec![]);
        }
        let rule = optimizer.rules()[self.rule_id].clone();
        trace!(event = "task_begin", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, rule = %rule.name());
        let group_id = optimizer.get_group_id(self.expr_id);
        let mut tasks = vec![];
        let binding_exprs = match_and_pick_group(rule.matcher(), group_id, optimizer);
        for expr in binding_exprs {
            let applied = rule.apply(expr);
            for expr in applied {
                let RelNode { typ, .. } = expr;
                if typ.extract_group().is_some() {
                    unreachable!();
                }
                let expr_typ = typ;
                let (_, expr_id) = optimizer.add_group_expr(expr.into(), Some(group_id));
                trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, new_expr_id = %expr_id);
                if expr_typ.is_logical() {
                    tasks.push(
                        Box::new(OptimizeExpressionTask::new(expr_id, self.exploring))
                            as Box<dyn Task<T>>,
                    );
                } else {
                    tasks
                        .push(Box::new(OptimizeInputsTask::new(expr_id, true)) as Box<dyn Task<T>>);
                }
            }
        }
        optimizer.mark_rule_fired(self.expr_id, self.rule_id);

        trace!(event = "task_end", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        Ok(tasks)
    }
}
