// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use itertools::Itertools;

use crate::nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup};
use crate::optimizer::Optimizer;
use crate::property::PropertyBuilderAny;
use crate::rules::{Rule, RuleMatcher};

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

pub struct HeuristicsOptimizer<T: NodeType> {
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    apply_order: ApplyOrder,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
    properties: HashMap<ArcPlanNode<T>, Arc<[Box<dyn Any + Send + Sync + 'static>]>>,
}

fn match_node<T: NodeType>(
    children: &[RuleMatcher<T>],
    node: ArcPlanNode<T>,
) -> Option<ArcPlanNode<T>> {
    let mut matched_children = Vec::new();
    if let [RuleMatcher::AnyMany] = children {
        return Some(node);
    }
    assert_eq!(children.len(), node.children.len(), "mismatched matcher");
    for (idx, child) in children.iter().enumerate() {
        match child {
            RuleMatcher::Any => {
                matched_children.push(node.child(idx));
            }
            RuleMatcher::AnyMany => {
                unreachable!();
            }
            _ => {
                if let Some(child) = match_and_pick(child, node.child_rel(idx)) {
                    matched_children.push(child.into());
                } else {
                    return None;
                }
            }
        }
    }
    Some(Arc::new(PlanNode {
        typ: node.typ.clone(),
        children: matched_children,
        predicates: node.predicates.clone(),
    }))
}

fn match_and_pick<T: NodeType>(
    matcher: &RuleMatcher<T>,
    node: ArcPlanNode<T>,
) -> Option<ArcPlanNode<T>> {
    match matcher {
        RuleMatcher::MatchNode { typ, children } => {
            if &node.typ != typ {
                return None;
            }
            match_node(children, node)
        }
        RuleMatcher::MatchDiscriminant {
            typ_discriminant,
            children,
        } => {
            if &std::mem::discriminant(&node.typ) != typ_discriminant {
                return None;
            }
            match_node(children, node)
        }
        _ => panic!("top node should be match node"),
    }
}

impl<T: NodeType> HeuristicsOptimizer<T> {
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

    fn optimize_inputs(
        &mut self,
        inputs: &[PlanNodeOrGroup<T>],
    ) -> Result<Vec<PlanNodeOrGroup<T>>> {
        let mut optimized_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            optimized_inputs.push(PlanNodeOrGroup::PlanNode(
                self.optimize_inner(input.clone().unwrap_plan_node())?,
            ));
        }
        Ok(optimized_inputs)
    }

    fn apply_rules(&mut self, mut root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        for rule in self.rules.clone().as_ref() {
            // Properties only matter for applying rules, therefore applying it before each rule
            // invoke.
            let matcher = rule.matcher();
            if let Some(binding) = match_and_pick(matcher, root_rel.clone()) {
                self.infer_properties(root_rel.clone());
                let mut results = rule.apply(self, binding);
                assert!(results.len() <= 1);
                if !results.is_empty() {
                    root_rel = results.remove(0).unwrap_plan_node();
                }
            }
        }
        Ok(root_rel)
    }

    fn optimize_inner(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        match self.apply_order {
            ApplyOrder::BottomUp => {
                let optimized_children = self.optimize_inputs(&root_rel.children)?;
                let node = self.apply_rules(
                    PlanNode {
                        typ: root_rel.typ.clone(),
                        children: optimized_children,
                        predicates: root_rel.predicates.clone(),
                    }
                    .into(),
                )?;
                Ok(node)
            }
            ApplyOrder::TopDown => {
                let root_rel = self.apply_rules(root_rel)?;
                let optimized_children = self.optimize_inputs(&root_rel.children)?;
                let node: Arc<PlanNode<T>> = PlanNode {
                    typ: root_rel.typ.clone(),
                    children: optimized_children,
                    predicates: root_rel.predicates.clone(),
                }
                .into();
                Ok(node)
            }
        }
    }

    fn infer_properties(&mut self, root_rel: ArcPlanNode<T>) {
        if self.properties.contains_key(&root_rel) {
            return;
        }

        let child_properties = root_rel
            .children
            .iter()
            .map(|child| {
                let child = child.unwrap_plan_node();
                self.infer_properties(child.clone());
                self.properties.get(&child).unwrap().clone()
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
                &root_rel.predicates,
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        self.properties.insert(root_rel.clone(), props.into());
    }
}

impl<T: NodeType> Optimizer<T> for HeuristicsOptimizer<T> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        self.optimize_inner(root_rel)
    }

    fn get_property<P: crate::property::PropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop {
        let props = self
            .properties
            .get(&root_rel.unwrap_plan_node())
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
