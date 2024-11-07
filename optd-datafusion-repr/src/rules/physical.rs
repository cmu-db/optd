// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use optd_core::nodes::{PlanNode, PlanNodeOrGroup};
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{ArcDfPlanNode, DfNodeType, JoinType};

pub struct PhysicalConversionRule {
    matcher: RuleMatcher<DfNodeType>,
}

impl PhysicalConversionRule {
    pub fn new(logical_typ: DfNodeType) -> Self {
        Self {
            matcher: RuleMatcher::MatchDiscriminant {
                typ_discriminant: std::mem::discriminant(&logical_typ),
                children: vec![RuleMatcher::AnyMany],
            },
        }
    }
}

impl PhysicalConversionRule {
    pub fn all_conversions<O: Optimizer<DfNodeType>>() -> Vec<Arc<dyn Rule<DfNodeType, O>>> {
        // Define conversions below, and add them to this list!
        // Note that we're using discriminant matching, so only one value of each variant
        // is sufficient to match all values of a variant.
        let rules: Vec<Arc<dyn Rule<DfNodeType, O>>> = vec![
            Arc::new(PhysicalConversionRule::new(DfNodeType::Scan)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Projection)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Filter)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Sort)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Agg)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::EmptyRelation)),
            Arc::new(PhysicalConversionRule::new(DfNodeType::Limit)),
        ];

        rules
    }
}

impl<O: Optimizer<DfNodeType>> Rule<DfNodeType, O> for PhysicalConversionRule {
    fn matcher(&self) -> &RuleMatcher<DfNodeType> {
        &self.matcher
    }

    fn apply(&self, _: &O, binding: ArcDfPlanNode) -> Vec<PlanNodeOrGroup<DfNodeType>> {
        let PlanNode {
            typ,
            children,
            predicates,
        } = Arc::unwrap_or_clone(binding);

        match typ {
            DfNodeType::Join(x) => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalNestedLoopJoin(x),
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::Scan => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalScan,
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::Filter => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalFilter,
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::Projection => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalProjection,
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::Sort => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalSort,
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::Agg => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalAgg,
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::EmptyRelation => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalEmptyRelation,
                    children,
                    predicates,
                };
                vec![node.into()]
            }
            DfNodeType::Limit => {
                let node = PlanNode {
                    typ: DfNodeType::PhysicalLimit,
                    children,
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
