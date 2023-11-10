use std::collections::HashMap;

use optd_core::rel_node::RelNode;
use optd_core::rules::{OneOrMany, Rule, RuleMatcher};

use crate::plan_nodes::{JoinType, OptRelNodeTyp};

/// Implements A join B = B join A
/// TODO: should insert a projection to reorder the columns
pub struct JoinCommuteRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

const LEFT_CHILD: usize = 0;
const RIGHT_CHILD: usize = 1;
const COND: usize = 2;
const JOIN_NODE: usize = 3;

impl JoinCommuteRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchAndPickNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                pick_to: JOIN_NODE,
                children: vec![
                    RuleMatcher::PickOne {
                        pick_to: LEFT_CHILD,
                    },
                    RuleMatcher::PickOne {
                        pick_to: RIGHT_CHILD,
                    },
                    RuleMatcher::PickOne { pick_to: COND },
                ],
            },
        }
    }
}

impl Rule<OptRelNodeTyp> for JoinCommuteRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, OneOrMany<RelNode<OptRelNodeTyp>>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let RelNode { typ, data, .. } = input.remove(&JOIN_NODE).unwrap().as_one() else {
            unreachable!()
        };
        let left_child = input.remove(&LEFT_CHILD).unwrap().as_one();
        let right_child = input.remove(&RIGHT_CHILD).unwrap().as_one();
        let cond = input.remove(&COND).unwrap().as_one();
        let node = RelNode {
            typ,
            children: vec![right_child.into(), left_child.into(), cond.into()],
            data,
        };
        vec![node.into()]
    }

    fn name(&self) -> &'static str {
        "join_commute"
    }
}
