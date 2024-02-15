use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{ConstantType, LogicalEmptyRelation, OptRelNode, OptRelNodeTyp};

use super::macros::define_rule;

define_rule!(
    EliminateFilterRule,
    apply_eliminate_filter,
    (Filter, child, [cond])
);

/// Transformations:
///     - Filter node w/ false pred -> EmptyRelation
///     - Filter node w/ true pred  -> Eliminate from the tree
fn apply_eliminate_filter(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateFilterRulePicks { child, cond }: EliminateFilterRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::Constant(ConstantType::Bool) = cond.typ {
        if let Some(data) = cond.data {
            if data.as_bool() {
                // If the condition is true, eliminate the filter node, as it
                // will yield everything from below it.
                return vec![child];
            } else {
                // If the condition is false, replace this node with the empty relation,
                // since it will never yield tuples.
                let node = LogicalEmptyRelation::new(false);
                return vec![node.into_rel_node().as_ref().clone()];
            }
        }
    }
    vec![]
}
