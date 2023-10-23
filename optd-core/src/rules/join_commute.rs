use crate::{
    plan_nodes::{LogicalJoin, OptRelNode, OptRelNodeRef, OptRelNodeTyp},
    rel_node::Value,
};

use super::Rule;

pub struct JoinCommuteRule {}

impl Rule<OptRelNodeTyp> for JoinCommuteRule {
    fn matches(&self, typ: OptRelNodeTyp, _data: Option<Value>) -> bool {
        return typ == OptRelNodeTyp::Join;
    }

    fn apply(&self, input: OptRelNodeRef) -> Vec<OptRelNodeRef> {
        let join = LogicalJoin::from_rel_node(input).unwrap();
        let new_node = LogicalJoin::new(
            join.right_child(),
            join.left_child(),
            join.cond(),
            join.join_type(),
        ); // TODO: convert cond and join type
        return vec![new_node.0.into_rel_node().into()];
    }

    fn name(&self) -> &'static str {
        "join_commute"
    }
}
