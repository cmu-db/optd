// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use itertools::Itertools;

use crate::logical_property::{LogicalProperty, LogicalPropertyBuilderAny};
use crate::nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup};
use crate::optimizer::Optimizer;
use crate::physical_property::{
    PhysicalProperty, PhysicalPropertyBuilderAny, PhysicalPropertyBuilders,
};
use crate::rules::{Rule, RuleMatcher};

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

pub struct HeuristicsOptimizerOptions {
    pub apply_order: ApplyOrder,
    pub enable_physical_prop_passthrough: bool,
}

pub struct HeuristicsOptimizer<T: NodeType> {
    rules: Arc<[Arc<dyn Rule<T, Self>>]>,
    options: HeuristicsOptimizerOptions,
    logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
    physical_property_builders: PhysicalPropertyBuilders<T>,
    logical_properties_cache: HashMap<ArcPlanNode<T>, Arc<[Box<dyn LogicalProperty>]>>,
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
        options: HeuristicsOptimizerOptions,
        logical_property_builders: Arc<[Box<dyn LogicalPropertyBuilderAny<T>>]>,
        physical_property_builders: Arc<[Box<dyn PhysicalPropertyBuilderAny<T>>]>,
    ) -> Self {
        Self {
            rules: rules.into(),
            options,
            logical_property_builders,
            logical_properties_cache: HashMap::new(),
            physical_property_builders: PhysicalPropertyBuilders(physical_property_builders),
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
        match self.options.apply_order {
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
        if self.logical_properties_cache.contains_key(&root_rel) {
            return;
        }

        let child_properties = root_rel
            .children
            .iter()
            .map(|child| {
                let child = child.unwrap_plan_node();
                self.infer_properties(child.clone());
                self.logical_properties_cache.get(&child).unwrap().clone()
            })
            .collect_vec();
        let mut props = Vec::with_capacity(self.logical_property_builders.len());
        for (id, builder) in self.logical_property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref())
                .collect::<Vec<_>>();
            let prop = builder.derive_any(
                root_rel.typ.clone(),
                &root_rel.predicates,
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        self.logical_properties_cache
            .insert(root_rel.clone(), props.into());
    }

    #[allow(clippy::type_complexity)]
    fn enforce_physical_properties_inner<X, Y>(
        &self,
        root_rel: ArcPlanNode<T>,
        required_props: Y,
    ) -> Result<(ArcPlanNode<T>, Vec<Box<dyn PhysicalProperty>>)>
    where
        X: Borrow<dyn PhysicalProperty>,
        Y: AsRef<[X]>,
    {
        let required_props = required_props.as_ref();
        let children_required_props = if self.options.enable_physical_prop_passthrough {
            // If physical property passthrough is enabled, we pass the required properties to the children.
            self.physical_property_builders.passthrough_many(
                root_rel.typ.clone(),
                &root_rel.predicates,
                required_props,
                root_rel.children.len(),
            )
        } else {
            // Otherwise, we do not pass any required property of the current node, and therefore, it only generates
            // the properties required by the node type.
            self.physical_property_builders
                .passthrough_many_no_required_property(
                    root_rel.typ.clone(),
                    &root_rel.predicates,
                    root_rel.children.len(),
                )
        };
        let mut children = Vec::with_capacity(root_rel.children.len());
        let mut children_output_properties = Vec::with_capacity(root_rel.children.len());
        for (child, required_properties) in root_rel
            .children
            .iter()
            .zip(children_required_props.into_iter())
        {
            let (child, child_output_properties) = self.enforce_physical_properties_inner(
                child.unwrap_plan_node(),
                &required_properties,
            )?;
            children.push(PlanNodeOrGroup::PlanNode(child));
            children_output_properties.push(child_output_properties);
        }
        let new_root_rel = Arc::new(PlanNode {
            typ: root_rel.typ.clone(),
            children,
            predicates: root_rel.predicates.clone(),
        });
        let derived_props = self.physical_property_builders.derive_many(
            root_rel.typ.clone(),
            &root_rel.predicates,
            &children_output_properties,
            root_rel.children.len(),
        );
        let (current_rel, output_properties) = self
            .physical_property_builders
            .enforce_many_if_not_satisfied(new_root_rel.into(), &derived_props, required_props);
        Ok((current_rel.unwrap_plan_node(), output_properties))
    }

    fn enforce_physical_properties(
        &self,
        root_rel: ArcPlanNode<T>,
        required_props: &[&dyn PhysicalProperty],
    ) -> Result<ArcPlanNode<T>> {
        assert_eq!(required_props.len(), self.physical_property_builders.len());
        let (root_rel, output_properties) =
            self.enforce_physical_properties_inner(root_rel, required_props)?;
        let _ = output_properties;
        Ok(root_rel)
    }
}

impl<T: NodeType> Optimizer<T> for HeuristicsOptimizer<T> {
    fn optimize(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        let phys_props = self.physical_property_builders.default_many();
        let phys_props_ref = phys_props.iter().map(|x| x.as_ref()).collect_vec();
        self.optimize_with_required_props(root_rel, &phys_props_ref)
    }

    fn optimize_with_required_props(
        &mut self,
        root_rel: ArcPlanNode<T>,
        required_props: &[&dyn PhysicalProperty],
    ) -> Result<ArcPlanNode<T>> {
        let optimized_rel = self.optimize_inner(root_rel)?;
        self.enforce_physical_properties(optimized_rel, required_props)
    }

    fn get_logical_property<P: crate::logical_property::LogicalPropertyBuilder<T>>(
        &self,
        root_rel: PlanNodeOrGroup<T>,
        idx: usize,
    ) -> P::Prop {
        let props = self
            .logical_properties_cache
            .get(&root_rel.unwrap_plan_node())
            .with_context(|| format!("cannot obtain properties for {}", root_rel))
            .unwrap();
        let prop = props[idx].as_ref();
        prop.as_any()
            .downcast_ref::<P::Prop>()
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
