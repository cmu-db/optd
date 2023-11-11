use std::collections::HashMap;

use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{JoinType, OptRelNodeTyp};

/// Implements (A join B) join C = A join (B join C)
pub struct JoinAssocRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

const CHILD_A: usize = 0;
const CHILD_B: usize = 1;
const CHILD_C: usize = 2;
const COND_AB: usize = 3;
const COND_BC: usize = 4;
const JOIN_NODE_TOP: usize = 5;
const JOIN_NODE_DOWN: usize = 6;

impl JoinAssocRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                pick_to: JOIN_NODE_TOP,
                children: vec![
                    RuleMatcher::PickOne { pick_to: CHILD_A },
                    RuleMatcher::MatchAndPickNode {
                        typ: OptRelNodeTyp::Join(JoinType::Inner),
                        children: vec![
                            RuleMatcher::PickOne { pick_to: CHILD_B },
                            RuleMatcher::PickOne { pick_to: CHILD_C },
                            RuleMatcher::PickOne { pick_to: COND_BC },
                        ],
                        pick_to: JOIN_NODE_DOWN,
                    },
                    RuleMatcher::PickOne { pick_to: COND_AB },
                ],
            },
        }
    }
}

impl Rule<OptRelNodeTyp> for JoinAssocRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, RelNode<OptRelNodeTyp>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let RelNode {
            typ: top_typ,
            data: top_data,
            ..
        } = input.remove(&JOIN_NODE_TOP).unwrap();
        let RelNode {
            typ: down_typ,
            data: down_data,
            ..
        } = input.remove(&JOIN_NODE_DOWN).unwrap();
        let child_a = input.remove(&CHILD_A).unwrap();
        let child_b = input.remove(&CHILD_B).unwrap();
        let child_c = input.remove(&CHILD_C).unwrap();
        let cond_ab = input.remove(&COND_AB).unwrap();
        let cond_bc = input.remove(&COND_BC).unwrap();
        let node = RelNode {
            typ: top_typ,
            children: vec![
                RelNode {
                    typ: down_typ,
                    children: vec![child_a.into(), child_b.into(), cond_ab.into()],
                    data: down_data,
                }
                .into(),
                child_c.into(),
                cond_bc.into(),
            ],
            data: top_data,
        };
        vec![node]
    }

    fn name(&self) -> &'static str {
        "join_assoc"
    }
}
