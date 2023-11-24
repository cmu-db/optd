use std::collections::HashMap;

use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::{JoinType, OptRelNodeTyp};

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
                                expand: false,
                            },
                            RuleMatcher::PickOne {
                                pick_to: FILTER_COND,
                                expand: true,
                            },
                        ],
                    },
                    RuleMatcher::PickOne {
                        pick_to: RIGHT_CHILD,
                        expand: false,
                    },
                    RuleMatcher::PickOne {
                        pick_to: JOIN_COND,
                        expand: true,
                    },
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
        mut input: HashMap<usize, RelNode<OptRelNodeTyp>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let left_child = input.remove(&LEFT_CHILD).unwrap();
        let right_child = input.remove(&RIGHT_CHILD).unwrap();
        let join_cond = input.remove(&JOIN_COND).unwrap();
        let filter_cond = input.remove(&FILTER_COND).unwrap();
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
        vec![filter_node]
    }

    fn name(&self) -> &'static str {
        "filter_join"
    }
}
