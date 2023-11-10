use std::collections::HashMap;

use crate::{
    plan_nodes::{JoinType, OptRelNodeTyp},
    rel_node::RelNode,
};

use super::{
    ir::{OneOrMany, RuleMatcher},
    Rule,
};

pub struct FilterJoinPullUpRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

const LEFT_CHILD: usize = 0;
const RIGHT_CHILD: usize = 1;
const JOIN_COND: usize = 2;
const FILTER_COND: usize = 3;

impl FilterJoinPullUpRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                children: vec![
                    RuleMatcher::MatchNode {
                        typ: OptRelNodeTyp::Filter,
                        children: vec![
                            RuleMatcher::PickOne {
                                pick_to: LEFT_CHILD,
                            },
                            RuleMatcher::PickOne {
                                pick_to: FILTER_COND,
                            },
                        ],
                    },
                    RuleMatcher::PickOne {
                        pick_to: RIGHT_CHILD,
                    },
                    RuleMatcher::PickOne { pick_to: JOIN_COND },
                ],
            },
        }
    }
}

impl Rule<OptRelNodeTyp> for FilterJoinPullUpRule {
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        mut input: HashMap<usize, OneOrMany<RelNode<OptRelNodeTyp>>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let left_child = input.remove(&LEFT_CHILD).unwrap().as_one();
        let right_child = input.remove(&RIGHT_CHILD).unwrap().as_one();
        let join_cond = input.remove(&JOIN_COND).unwrap().as_one();
        let filter_cond = input.remove(&FILTER_COND).unwrap().as_one();
        let join_node = RelNode {
            typ: OptRelNodeTyp::Join(JoinType::Inner),
            children: vec![left_child.into(), right_child.into(), join_cond.into()],
            data: None,
        };
        let filter_node = RelNode {
            typ: OptRelNodeTyp::Filter,
            children: vec![join_node.into(), filter_cond.into()],
            data: None,
        };
        vec![filter_node.into()]
    }

    fn name(&self) -> &'static str {
        "join_commute"
    }
}
