use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;
use tracing::trace;

use crate::{
    cascades::{
        memo::{BindingType, RelMemoNodeRef},
        optimizer::{rule_matches_expr, ExprId, RuleId},
        tasks::{explore_expr::ExploreExprTask, optimize_inputs::OptimizeInputsTask},
        CascadesOptimizer, GroupId,
    },
    rel_node::{RelNode, RelNodeTyp},
    rules::{Rule, RuleMatcher},
};

use super::Task;

// Pick/match logic, to get pieces of info to pass to the rule apply function
// TODO: I would like to see this moved elsewhere

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
                    let exprs = optimizer.get_all_exprs_in_group(group_id);

                    let mut bindings = exprs
                        .into_iter()
                        .map(|expr| {
                            optimizer
                                .get_all_expr_bindings(expr, BindingType::Logical, None)
                                .into_iter()
                                .filter(|y| y.typ.is_logical())
                                .collect_vec()
                        })
                        .flatten()
                        .collect_vec();
                    assert_eq!(bindings.len(), 1, "can only expand expression");
                    bindings.remove(0).as_ref().clone()
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
        RuleMatcher::MatchDiscriminant {
            typ_discriminant,
            children,
        } => {
            if std::mem::discriminant(&node.typ) != *typ_discriminant {
                return vec![];
            }
            match_node(&node.typ.clone(), children, None, node, optimizer)
        }
        RuleMatcher::MatchAndPickDiscriminant {
            typ_discriminant,
            children,
            pick_to,
        } => {
            if std::mem::discriminant(&node.typ) != *typ_discriminant {
                return vec![];
            }
            match_node(&node.typ.clone(), children, Some(*pick_to), node, optimizer)
        }
        _ => panic!("top node should be match node"),
    }
}

pub struct ApplyRuleTask<T: RelNodeTyp> {
    parent_task_id: Option<usize>,
    task_id: usize,
    expr_id: ExprId,
    rule_id: RuleId,
    rule: Arc<dyn Rule<T, CascadesOptimizer<T>>>,
    // TODO: Promise here? Maybe it can be part of the Rule trait.
    cost_limit: Option<isize>,
}

impl<T: RelNodeTyp> ApplyRuleTask<T> {
    pub fn new(
        parent_task_id: Option<usize>,
        task_id: usize,
        expr_id: ExprId,
        rule_id: RuleId,
        rule: Arc<dyn Rule<T, CascadesOptimizer<T>>>,
        cost_limit: Option<isize>,
    ) -> Self {
        Self {
            parent_task_id,
            task_id,
            expr_id,
            rule_id,
            rule,
            cost_limit,
        }
    }
}

fn transform<T: RelNodeTyp>(
    optimizer: &CascadesOptimizer<T>,
    expr_id: ExprId,
    rule: &Arc<dyn Rule<T, CascadesOptimizer<T>>>,
) -> Vec<RelNode<T>> {
    let picked_datas = match_and_pick_expr(rule.matcher(), expr_id, optimizer);

    if picked_datas.is_empty() {
        vec![]
    } else {
        picked_datas
            .into_iter()
            .map(|picked_data| rule.apply(optimizer, picked_data))
            .flatten()
            .collect()
    }
}

fn update_memo<T: RelNodeTyp>(
    optimizer: &CascadesOptimizer<T>,
    group_id: GroupId,
    new_exprs: Vec<Arc<RelNode<T>>>,
) -> Vec<ExprId> {
    let mut expr_ids = vec![];
    for new_expr in new_exprs {
        if let Some(_) = new_expr.typ.extract_group() {
            // TODO: handle "merge group" case
            todo!("merge group case");
        }
        let (_, expr_id) = optimizer.add_expr_to_group(new_expr, group_id);
        expr_ids.push(expr_id);
    }
    expr_ids
}

/// TODO
///
/// Pseudocode:
/// function ApplyRule(expr, rule, promise, limit)
///     newExprs ← Transform(expr,rule)
///     UpdateMemo(newExprs)
///     Sort exprs by promise
///     for newExpr ∈ newExprs do
///         if Rule is a transformation rule then
///             tasks.Push(ExplExpr(newExpr, limit))
///         else
///             // Can fail if the cost limit becomes 0 or negative
///             limit ← UpdateCostLimit(newExpr, limit)
///             tasks.Push(OptInputs(newExpr, limit))
impl<T: RelNodeTyp> Task<T> for ApplyRuleTask<T> {
    fn execute(&self, optimizer: &CascadesOptimizer<T>) {
        let expr = optimizer.get_expr_memoed(self.expr_id);

        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_begin", task = "apply_rule", rule_id = %self.rule_id, rule = %self.rule.name(), expr_id = %self.expr_id, expr = %expr);

        let group_id = optimizer.get_group_id(self.expr_id);

        debug_assert!(rule_matches_expr(&self.rule, &expr));

        let new_exprs = transform(optimizer, self.expr_id, &self.rule);
        let new_exprs = new_exprs.into_iter().map(Arc::new).collect();
        let new_expr_ids = update_memo(optimizer, group_id, new_exprs);
        // TODO sort exprs by promise
        for new_expr_id in new_expr_ids {
            let is_transformation_rule = !self.rule.is_impl_rule();
            if is_transformation_rule {
                // TODO: Increment transformation count
                optimizer.push_task(Box::new(ExploreExprTask::new(
                    Some(self.task_id),
                    optimizer.get_next_task_id(),
                    new_expr_id,
                    self.cost_limit,
                )));
            } else {
                // TODO: Also, make cost limit optional with parameters struct like before
                let new_limit = None; // TODO: How do we update cost limit
                optimizer.push_task(Box::new(OptimizeInputsTask::new(
                    Some(self.task_id),
                    optimizer.get_next_task_id(),
                    new_expr_id,
                    new_limit,
                )));
            }
        }
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_finish", task = "apply_rule", rule_id = %self.rule_id, rule = %self.rule.name(), expr_id = %self.expr_id, expr = %expr);
    }
}
