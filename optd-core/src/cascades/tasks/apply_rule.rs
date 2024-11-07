use std::sync::Arc;

use anyhow::Result;
use itertools::Itertools;
use tracing::trace;

use super::Task;
use crate::cascades::memo::ArcMemoPlanNode;
use crate::cascades::optimizer::{CascadesOptimizer, ExprId, RuleId};
use crate::cascades::tasks::{OptimizeExpressionTask, OptimizeInputsTask};
use crate::cascades::{GroupId, Memo};
use crate::nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup};
use crate::rules::{OptimizeType, RuleMatcher};

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

// Pick/match logic, to get pieces of info to pass to the rule apply function
// TODO: I would like to see this moved elsewhere

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

fn match_and_pick_expr<T: NodeType, M: Memo<T>>(
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

impl<T: NodeType, M: Memo<T>> Task<T, M> for ApplyRuleTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) -> Result<Vec<Box<dyn Task<T, M>>>> {
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
        for binding in binding_exprs {
            trace!(event = "before_apply_rule", task = "apply_rule", input_binding=%binding);
            let applied = rule.apply(optimizer, binding);

            if rule_wrapper.optimize_type() == OptimizeType::Heuristics {
                panic!("no more heuristics rule in cascades");
            }

            for expr in applied {
                trace!(event = "after_apply_rule", task = "apply_rule", output_binding=%expr);
                // TODO: remove clone in the below line
                if let Some(expr_id) = optimizer.add_expr_to_group(expr.clone(), group_id) {
                    let typ = expr.unwrap_typ();
                    if typ.is_logical() {
                        tasks.push(
                            Box::new(OptimizeExpressionTask::new(expr_id, self.exploring))
                                as Box<dyn Task<T, M>>,
                        );
                    } else {
                        tasks.push(Box::new(OptimizeInputsTask::new(
                            expr_id,
                            !optimizer.prop.disable_pruning,
                        )) as Box<dyn Task<T, M>>);
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
