use std::collections::HashMap;
use std::sync::Arc;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::{MaybeRelNode, RelNode};
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{JoinType, OptRelNodeTyp};

pub struct PhysicalConversionRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

impl PhysicalConversionRule {
    pub fn new(logical_typ: OptRelNodeTyp) -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: logical_typ,
                pick_to: 0,
                children: vec![RuleMatcher::IgnoreMany],
            },
        }
    }
}

impl PhysicalConversionRule {
    pub fn all_conversions<O: Optimizer<OptRelNodeTyp>>() -> Vec<Arc<dyn Rule<OptRelNodeTyp, O>>> {
        // Define conversions below, and add them to this list!
        vec![
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Scan)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Projection)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::LeftOuter,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::Cross,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Filter)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Sort)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Agg)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::EmptyRelation)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Limit)),
        ]
    }
}

impl<O: Optimizer<OptRelNodeTyp>> Rule<OptRelNodeTyp, O> for PhysicalConversionRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        _optimizer: &O,
        mut input: HashMap<usize, MaybeRelNode<OptRelNodeTyp>>,
    ) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
        let MaybeRelNode::RelNode(rel) = input.remove(&0).unwrap() else {
            return vec![];
        };

        let RelNode {
            typ,
            data,
            children,
            predicates,
        } = rel.as_ref().clone();

        match typ {
            OptRelNodeTyp::Join(x) => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x),
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::Scan => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalScan,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::Filter => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalFilter,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::Projection => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalProjection,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::Sort => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalSort,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::Agg => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalAgg,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::EmptyRelation => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalEmptyRelation,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            OptRelNodeTyp::Limit => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalLimit,
                    children,
                    data,
                    predicates,
                };
                vec![node.into()]
            }
            _ => vec![],
        }
    }

    fn is_impl_rule(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "physical_conversion"
    }
}
