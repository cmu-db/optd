use std::collections::HashMap;

use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::OptRelNodeTyp;

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

impl Rule<OptRelNodeTyp> for PhysicalConversionRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, RelNode<OptRelNodeTyp>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let RelNode {
            typ,
            data,
            children,
        } = input.remove(&0).unwrap();

        match typ {
            OptRelNodeTyp::Apply(x) => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x.to_join_type()),
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Join(x) => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalNestedLoopJoin(x),
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Scan => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalScan,
                    children,
                    data,
                };
                vec![node]
            }
            OptRelNodeTyp::Filter => {
                let node = RelNode {
                    typ: OptRelNodeTyp::PhysicalFilter,
                    children,
                    data,
                };
                vec![node]
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
