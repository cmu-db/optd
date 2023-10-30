use crate::{
    plan_nodes::{LogicalFilter, LogicalJoin, OptRelNode, OptRelNodeRef, OptRelNodeTyp},
    rel_node::Value,
};

use super::Rule;

pub struct FilterJoinRule {}

impl Rule<OptRelNodeTyp> for FilterJoinRule {
    fn matches(&self, typ: OptRelNodeTyp, _data: Option<Value>) -> bool {
        matches!(typ, OptRelNodeTyp::Join(_))
    }

    fn apply(&self, input: OptRelNodeRef) -> Vec<OptRelNodeRef> {
        let join: LogicalJoin = LogicalJoin::from_rel_node(input).unwrap();
        if let Some(filter) = LogicalFilter::from_rel_node(join.left_child().into_rel_node()) {
            let child = filter.child();
            let new_node = LogicalFilter::new(
                LogicalJoin::new(child, join.right_child(), join.cond(), join.join_type()).0,
                filter.cond(),
            ); // TODO: convert cond and join type
            return vec![new_node.0.into_rel_node()];
        }
        vec![]
    }

    fn name(&self) -> &'static str {
        "filter_join_pull_up"
    }
}
