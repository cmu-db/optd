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
    rules::{OptimizeType, RuleMatcher},
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
            RuleMatcher::PickOne { pick_to, expand } => {
                let group_id = node.children[idx];
                let node = if *expand {
                    let binding = optimizer
                        .get_predicate_binding(group_id)
                        .expect("empty group, what's going wrong?");
                    binding.as_ref().clone()
                } else {
                    RelNode::new_group(group_id)
                };
                for pick in &mut picks {
                    let res = pick.insert(*pick_to, node.clone());
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
                    typ: typ.clone(),
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

fn match_and_pick_expr<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    expr_id: ExprId,
    optimizer: &CascadesOptimizer<T>,
) -> Vec<HashMap<usize, RelNode<T>>> {
    let node = optimizer.get_expr_memoed(expr_id);
    match_and_pick(matcher, node, optimizer)
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

        if optimizer.is_rule_disabled(self.rule_id) {
            optimizer.mark_rule_fired(self.expr_id, self.rule_id);
            return Ok(vec![]);
        }

        let rule_wrapper = optimizer.rules()[self.rule_id].clone();
        let rule = rule_wrapper.rule();

        trace!(event = "task_begin", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, rule = %rule.name(), optimize_type=%rule_wrapper.optimize_type());
        let group_id = optimizer.get_group_id(self.expr_id);
        let mut tasks = vec![];
        let binding_exprs = match_and_pick_expr(rule.matcher(), self.expr_id, optimizer);
        for expr in binding_exprs {
            trace!(event = "before_apply_rule", task = "apply_rule", binding = ?expr.iter().map(|(k, v)| format!("{}=>{}", k, v)).join(","));
            let applied = rule.apply(optimizer, expr);

            if rule_wrapper.optimize_type() == OptimizeType::Heuristics {
                panic!("no more heuristics rule in cascades");
            }

            for expr in applied {
                trace!(event = "after_apply_rule", task = "apply_rule", binding=%expr);
                let expr_typ = expr.typ.clone();
                if let Some(expr_id) = optimizer.add_expr_to_group(expr.into(), group_id) {
                    if expr_typ.is_logical() {
                        tasks.push(
                            Box::new(OptimizeExpressionTask::new(expr_id, self.exploring))
                                as Box<dyn Task<T>>,
                        );
                    } else {
                        tasks
                            .push(Box::new(OptimizeInputsTask::new(expr_id, true))
                                as Box<dyn Task<T>>);
                    }
                    optimizer.unmark_expr_explored(expr_id);
                    trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, new_expr_id = %expr_id);
                } else {
                    trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, "triggered group merge");
                }
            }
        }
        optimizer.mark_rule_fired(self.expr_id, self.rule_id);

        trace!(event = "task_end", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        Ok(tasks)
    }

    fn describe(&self) -> String {
        format!(
            "apply_rule {{ rule_id: {}, expr_id: {}, exploring: {} }}",
            self.rule_id, self.expr_id, self.exploring
        )
    }
}
