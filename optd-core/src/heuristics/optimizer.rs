use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use itertools::Itertools;
use std::any::Any;

use crate::{
    optimizer::Optimizer,
    property::PropertyBuilderAny,
    rel_node::{RelNode, RelNodeRef, RelNodeTyp},
    rules::{Rule, RuleMatcher},
};

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

pub struct HeuristicsOptimizer<T: RelNodeTyp> {
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    apply_order: ApplyOrder,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    properties: HashMap<RelNodeRef<T>, Arc<[Box<dyn Any + Send + Sync + 'static>]>>,
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
            RuleMatcher::PickOne { pick_to, expand: _ } => {
                // Heuristics always keep the full plan without group placeholders, therefore we can ignore expand property.
                let res = pick.insert(*pick_to, node.child(idx).as_ref().clone());
                assert!(res.is_none(), "dup pick");
            }
            RuleMatcher::PickMany { pick_to } => {
                let res = pick.insert(*pick_to, RelNode::new_list(node.children[idx..].to_vec()));
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
    pub fn new_with_rules(
        rules: Vec<Arc<dyn Rule<T, Self>>>,
        apply_order: ApplyOrder,
        property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    ) -> Self {
        Self {
            rules: rules.into(),
            apply_order,
            property_builders,
            properties: HashMap::new(),
        }
    }

    fn optimize_inputs(&mut self, inputs: &[RelNodeRef<T>]) -> Result<Vec<RelNodeRef<T>>> {
        let mut optimized_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            optimized_inputs.push(self.optimize_inner(input.clone())?);
        }
        Ok(optimized_inputs)
    }

    fn apply_rules(&mut self, mut root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        for rule in self.rules.clone().as_ref() {
            // Properties only matter for applying rules, therefore applying it before each rule invoke.
            let matcher = rule.matcher();
            if let Some(picks) = match_and_pick(matcher, root_rel.clone()) {
                self.infer_properties(root_rel.clone());
                let mut results = rule.apply(self, picks);
                assert!(results.len() <= 1);
                if !results.is_empty() {
                    root_rel = results.remove(0).into();
                }
            }
        }
        Ok(root_rel)
    }

    fn optimize_inner(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
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
                let node: Arc<RelNode<T>> = RelNode {
                    typ: root_rel.typ.clone(),
                    children: optimized_children,
                    data: root_rel.data.clone(),
                }
                .into();
                Ok(node)
            }
        }
    }

    fn infer_properties(&mut self, root_rel: RelNodeRef<T>) {
        if self.properties.contains_key(&root_rel) {
            return;
        }

        let child_properties = root_rel
            .children
            .iter()
            .map(|child| {
                self.infer_properties((*child).clone());
                self.properties.get(child).unwrap().clone()
            })
            .collect_vec();
        let mut props = Vec::with_capacity(self.property_builders.len());
        for (id, builder) in self.property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref() as &dyn std::any::Any)
                .collect::<Vec<_>>();
            let prop = builder.derive_any(
                root_rel.typ.clone(),
                root_rel.data.clone(),
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        self.properties.insert(root_rel.clone(), props.into());
    }
}

impl<T: RelNodeTyp> Optimizer<T> for HeuristicsOptimizer<T> {
    fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: crate::property::PropertyBuilder<T>>(
        &self,
        root_rel: RelNodeRef<T>,
        idx: usize,
    ) -> P::Prop {
        let props = self
            .properties
            .get(&root_rel)
            .with_context(|| format!("cannot obtain properties for {}", root_rel))
            .unwrap();
        let prop = props[idx].as_ref();
        prop.downcast_ref::<P::Prop>()
            .with_context(|| {
                format!(
                    "cannot downcast property at idx {} into provided property instance",
                    idx
                )
            })
            .unwrap()
            .clone()
    }
}
