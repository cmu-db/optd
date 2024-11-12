use std::sync::Arc;

use itertools::Itertools;
use tracing::trace;

use crate::{
    cascades::{
        memo::ArcMemoPlanNode,
        optimizer::{rule_matches_expr, ExprId, RuleId},
        tasks::{explore_expr::ExploreExprTask, optimize_inputs::OptimizeInputsTask},
        CascadesOptimizer, GroupId, Memo,
    },
    nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup},
    rules::{Rule, RuleMatcher},
};

use super::Task;

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

pub struct ApplyRuleTask<T: NodeType, M: Memo<T>> {
    parent_task_id: Option<usize>,
    task_id: usize,
    expr_id: ExprId,
    rule_id: RuleId,
    rule: Arc<dyn Rule<T, CascadesOptimizer<T, M>>>,
    // TODO: Promise here? Maybe it can be part of the Rule trait.
    cost_limit: Option<isize>,
}

impl<T: NodeType, M: Memo<T>> ApplyRuleTask<T, M> {
    pub fn new(
        parent_task_id: Option<usize>,
        task_id: usize,
        expr_id: ExprId,
        rule_id: RuleId,
        rule: Arc<dyn Rule<T, CascadesOptimizer<T, M>>>,
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

fn transform<T: NodeType, M: Memo<T>>(
    optimizer: &CascadesOptimizer<T, M>,
    expr_id: ExprId,
    rule: &Arc<dyn Rule<T, CascadesOptimizer<T, M>>>,
) -> Vec<PlanNodeOrGroup<T>> {
    let picked_datas = match_and_pick_expr(rule.matcher(), expr_id, optimizer);

    if picked_datas.is_empty() {
        vec![]
    } else {
        picked_datas
            .into_iter()
            .flat_map(|picked_data| rule.apply(optimizer, picked_data))
            .collect()
    }
}

fn update_memo<T: NodeType, M: Memo<T>>(
    optimizer: &mut CascadesOptimizer<T, M>,
    group_id: GroupId,
    new_exprs: Vec<PlanNodeOrGroup<T>>,
) -> Vec<ExprId> {
    let mut expr_ids = vec![];
    for new_expr in new_exprs {
        if let Some(expr_id) = optimizer.add_expr_to_group(new_expr, group_id) {
            expr_ids.push(expr_id);
        }
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
impl<T: NodeType, M: Memo<T>> Task<T, M> for ApplyRuleTask<T, M> {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T, M>) {
        let expr = optimizer.get_expr_memoed(self.expr_id);

        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_begin", task = "apply_rule", rule_id = %self.rule_id, rule = %self.rule.name(), expr_id = %self.expr_id, expr = %expr);

        let group_id = optimizer.get_group_id(self.expr_id);

        debug_assert!(rule_matches_expr(&self.rule, &expr));

        let new_exprs = transform(optimizer, self.expr_id, &self.rule);
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
