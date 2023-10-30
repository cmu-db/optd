use itertools::Itertools;

use crate::{
    plan_nodes::{LogicalJoin, OptRelNode, OptRelNodeRef, OptRelNodeTyp},
    rel_node::Value,
};

use super::Rule;

/// Implements A join B = B join A
/// TODO: should insert a projection to reorder the columns
pub struct JoinCommuteRule {}

impl Rule<OptRelNodeTyp> for JoinCommuteRule {
    fn matches(&self, typ: OptRelNodeTyp, _data: Option<Value>) -> bool {
        matches!(typ, OptRelNodeTyp::Join(_))
    }

    fn apply(&self, input: OptRelNodeRef) -> Vec<OptRelNodeRef> {
        let join = LogicalJoin::from_rel_node(input).unwrap();
        let new_node = LogicalJoin::new(
            join.right_child(),
            join.left_child(),
            join.cond(),
            join.join_type(),
        ); // TODO: convert cond and join type
        vec![new_node.0.into_rel_node()]
    }

    fn name(&self) -> &'static str {
        "join_commute"
    }
}

/// Implements A join (B join C) = (A join B) join C
pub struct JoinAssocRule {}

impl Rule<OptRelNodeTyp> for JoinAssocRule {
    fn matches(&self, typ: OptRelNodeTyp, _data: Option<Value>) -> bool {
        matches!(typ, OptRelNodeTyp::Join(_))
    }

    fn apply(&self, input: OptRelNodeRef) -> Vec<OptRelNodeRef> {
        let join = LogicalJoin::from_rel_node(input).unwrap();
        let mut results = vec![];
        // (A join B) join C -> A join (B join C)
        if let Some(left_node) = LogicalJoin::from_rel_node(join.left_child().into_rel_node()) {
            let a = left_node.left_child();
            let b = left_node.right_child();
            let c = join.right_child();
            results.push(LogicalJoin::new(
                a,
                LogicalJoin::new(b, c, join.cond(), join.join_type()).0,
                left_node.cond(),
                left_node.join_type(),
            ))
        }
        // A join (B join C) -> (A join B) join C
        if let Some(right_node) = LogicalJoin::from_rel_node(join.right_child().into_rel_node()) {
            let a = join.left_child();
            let b = right_node.left_child();
            let c = right_node.right_child();
            results.push(LogicalJoin::new(
                LogicalJoin::new(a, b, join.cond(), join.join_type()).0,
                c,
                right_node.cond(),
                right_node.join_type(),
            )) // TODO(chi): is this rule correct??? cond should be rewritten?
        }

        results
            .into_iter()
            .map(|x| x.0.into_rel_node())
            .collect_vec()
    }

    fn name(&self) -> &'static str {
        "join_assoc"
    }
}
