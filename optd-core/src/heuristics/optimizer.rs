use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use itertools::Itertools;
use std::any::Any;

use crate::{
    nodes::{ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup, PredNode},
    optimizer::Optimizer,
    property::PropertyBuilderAny,
    rules::{Rule, RuleMatcher},
};

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
    typ: &T,
    children: &[RuleMatcher<T>],
    predicates: &[RuleMatcher<T>],
    pick_to: Option<usize>,
    node: ArcPlanNode<T>,
) -> Option<(
    HashMap<usize, PlanNodeOrGroup<T>>,
    HashMap<usize, ArcPredNode<T>>,
)> {
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
    let mut pick: HashMap<usize, PlanNodeOrGroup<T>> = HashMap::new();
    for (idx, child) in children.iter().enumerate() {
        assert!(!should_end, "many matcher should be at the end");
        match child {
            RuleMatcher::IgnoreOne => {}
            RuleMatcher::IgnoreMany => {
                should_end = true;
            }
            RuleMatcher::PickOne { pick_to } => {
                // Heuristics always keep the full plan without group placeholders, therefore we can ignore expand property.
                let res = pick.insert(*pick_to, node.child(idx));
                assert!(res.is_none(), "dup pick");
            }
            RuleMatcher::PickMany { pick_to } => {
                panic!("PickMany not supported currently");
                // let res = pick.insert(*pick_to, PlanNode::new_list(node.children[idx..].to_vec()));
                // assert!(res.is_none(), "dup pick");
                // should_end = true;
            }
            _ => {
                if let Some((new_picks, new_pred_picks)) = match_and_pick(child, node.child(idx)) {
                    pick.extend(new_picks.iter().map(|(k, v)| (*k, v.clone())));
                } else {
                    return None;
                }
            }
        }
    }
    let mut pred_pick: HashMap<usize, ArcPredNode<T>> = HashMap::new();
    for (idx, pred) in predicates.iter().enumerate() {
        match pred {
            RuleMatcher::PickPred { pick_to } => {
                let res = pred_pick.insert(*pick_to, node.predicate(idx));
                assert!(res.is_none(), "dup pick");
            }
            _ => {
                panic!("only PickPred is supported for predicates");
            }
        }
    }
    if let Some(pick_to) = pick_to {
        let res: Option<PlanNodeOrGroup<T>> = pick.insert(
            pick_to,
            PlanNodeOrGroup::PlanNode(
                PlanNode {
                    typ: typ.clone(),
                    children: node.children.clone(),
                    predicates: node.predicates.clone(),
                }
                .into(),
            ),
        );
        assert!(res.is_none(), "dup pick");
    }
    Some((pick, pred_pick))
}

fn match_and_pick<T: NodeType>(
    matcher: &RuleMatcher<T>,
    node: PlanNodeOrGroup<T>,
) -> Option<(
    HashMap<usize, PlanNodeOrGroup<T>>,
    HashMap<usize, ArcPredNode<T>>,
)> {
    match matcher {
        RuleMatcher::MatchAndPickNode {
            typ,
            children,
            predicates,
            pick_to,
        } => {
            let node = match node {
                PlanNodeOrGroup::PlanNode(node) => node,
                PlanNodeOrGroup::Group(_) => return None,
            };
            if &node.typ != typ {
                return None;
            }
            match_node(typ, children, predicates, Some(*pick_to), node)
        }
        RuleMatcher::MatchNode {
            typ,
            children,
            predicates,
        } => {
            let node = match node {
                PlanNodeOrGroup::PlanNode(node) => node,
                PlanNodeOrGroup::Group(_) => return None,
            };
            if &node.typ != typ {
                return None;
            }
            match_node(typ, children, predicates, None, node)
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

    fn optimize_inputs(&mut self, inputs: &[PlanNodeOrGroup<T>]) -> Result<Vec<ArcPlanNode<T>>> {
        let mut optimized_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            let input = input.unwrap_plan_node();
            optimized_inputs.push(self.optimize_inner(input.clone())?);
        }
        Ok(optimized_inputs)
    }

    fn apply_rules(&mut self, mut root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        for rule in self.rules.clone().as_ref() {
            // Properties only matter for applying rules, therefore applying it before each rule invoke.
            let matcher = rule.matcher();
            if let Some((picks, pick_preds)) =
                match_and_pick(matcher, PlanNodeOrGroup::PlanNode(root_rel.clone()))
            {
                let picks = picks.into_iter().map(|(k, v)| (k, v)).collect(); // This is kinda ugly, but it works for now
                self.infer_properties(root_rel.clone());
                let mut results = rule.apply(self, picks, pick_preds);
                assert!(results.len() <= 1);
                if !results.is_empty() {
                    root_rel = results.remove(0).into();
                }
            }
        }
        Ok(root_rel)
    }

    fn optimize_inner(&mut self, root_rel: ArcPlanNode<T>) -> Result<ArcPlanNode<T>> {
        match self.apply_order {
            ApplyOrder::BottomUp => {
                let optimized_children = self
                    .optimize_inputs(&root_rel.children)?
                    .into_iter()
                    .map(|x| PlanNodeOrGroup::PlanNode(x))
                    .collect();
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
                let optimized_children = self
                    .optimize_inputs(&root_rel.children)?
                    .into_iter()
                    .map(|x| PlanNodeOrGroup::PlanNode(x))
                    .collect();
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
                let plan_node = child.unwrap_plan_node();
                self.infer_properties(plan_node.clone());
                self.properties.get(&plan_node).unwrap().clone()
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
        root_rel: ArcPlanNode<T>,
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
