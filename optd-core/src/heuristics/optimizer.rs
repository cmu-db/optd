use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use crate::{
    rel_node::{RelNode, RelNodeRef, RelNodeTyp},
    rules::{Rule, RuleMatcher},
};

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

pub struct HeuristicsOptimizer<T: RelNodeTyp> {
    rules: Arc<[Arc<dyn Rule<T>>]>,
    apply_order: ApplyOrder,
}

fn match_node<T: RelNodeTyp>(
    typ: &T,
    children: &[RuleMatcher<T>],
    pick_to: Option<usize>,
    node: RelNodeRef<T>,
) -> Option<HashMap<usize, RelNode<T>>> {
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
    let mut pick = HashMap::new();
    for (idx, child) in children.iter().enumerate() {
        assert!(!should_end, "many matcher should be at the end");
        match child {
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                should_end = true;
            }
            RuleMatcher::PickOne { pick_to } => {
                let res = pick.insert(*pick_to, node.child(idx).as_ref().clone());
                assert!(res.is_none(), "dup pick");
            }
            RuleMatcher::PickMany { pick_to } => {
                let res = pick.insert(
                    *pick_to,
                    RelNode::new_list(node.children[idx..].to_vec()).into(),
                );
                assert!(res.is_none(), "dup pick");
                should_end = true;
            }
            _ => {
                if let Some(new_picks) = match_and_pick(child, node.child(idx)) {
                    pick.extend(new_picks.iter().map(|(k, v)| (*k, v.clone())));
                } else {
                    return None;
                }
            }
        }
    }
    if let Some(pick_to) = pick_to {
        let res: Option<RelNode<T>> = pick.insert(
            pick_to,
            RelNode {
                typ: typ.clone(),
                children: node.children.clone(),
                data: node.data.clone(),
            },
        );
        assert!(res.is_none(), "dup pick");
    }
    Some(pick)
}

fn match_and_pick<T: RelNodeTyp>(
    matcher: &RuleMatcher<T>,
    node: RelNodeRef<T>,
) -> Option<HashMap<usize, RelNode<T>>> {
    match matcher {
        RuleMatcher::MatchAndPickNode {
            typ,
            children,
            pick_to,
        } => {
            if &node.typ != typ {
                return None;
            }
            match_node(typ, children, Some(*pick_to), node)
        }
        RuleMatcher::MatchNode { typ, children } => {
            if &node.typ != typ {
                return None;
            }
            match_node(typ, children, None, node)
        }
        _ => panic!("top node should be match node"),
    }
}

impl<T: RelNodeTyp> HeuristicsOptimizer<T> {
    pub fn new_with_rules(rules: Vec<Arc<dyn Rule<T>>>, apply_order: ApplyOrder) -> Self {
        Self {
            rules: rules.into(),
            apply_order,
        }
    }

    fn optimize_inputs(&mut self, inputs: &[RelNodeRef<T>]) -> Result<Vec<RelNodeRef<T>>> {
        let mut optimized_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            optimized_inputs.push(self.optimize(input.clone())?);
        }
        Ok(optimized_inputs)
    }

    fn apply_rules(&mut self, mut root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        for rule in self.rules.as_ref() {
            let matcher = rule.matcher();
            if let Some(picks) = match_and_pick(matcher, root_rel.clone()) {
                let mut results = rule.apply(picks);
                assert_eq!(results.len(), 1);
                root_rel = results.remove(0).into();
            }
        }
        Ok(root_rel)
    }

    pub fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        match self.apply_order {
            ApplyOrder::BottomUp => {
                let optimized_children = self.optimize_inputs(&root_rel.children)?;
                let node = self.apply_rules(
                    RelNode {
                        typ: root_rel.typ.clone(),
                        children: optimized_children,
                        data: root_rel.data.clone(),
                    }
                    .into(),
                )?;
                Ok(node)
            }
            ApplyOrder::TopDown => {
                let root_rel = self.apply_rules(root_rel)?;
                let optimized_children = self.optimize_inputs(&root_rel.children)?;
                Ok(RelNode {
                    typ: root_rel.typ.clone(),
                    children: optimized_children,
                    data: root_rel.data.clone(),
                }
                .into())
            }
        }
    }
}
