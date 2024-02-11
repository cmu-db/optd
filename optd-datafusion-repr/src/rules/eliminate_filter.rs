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

/// Replace filter nodes with a false predicate
/// with an EmptyRelation (since it will yield no tuples)
fn apply_eliminate_filter(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateFilterRulePicks { child, cond }: EliminateFilterRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // If the conditional is a constant boolean
    if let OptRelNodeTyp::Constant(const_type) = cond.typ {
        if const_type == ConstantType::Bool {
            if let Some(data) = cond.data {
                if data.as_bool() {
                    // If the condition is true, eliminate the filter node, as it
                    // will yield everything from below it.
                    // TODO(bowad)
                    return vec![child];
                } else {
                    // If the condition is false, replace this node with the empty relation,
                    // since it will never yield tuples.
                    let node = LogicalEmptyRelation::new(false);
                    return vec![node.into_rel_node().as_ref().clone()];
                }
            }
        }
    }
    vec![]
}
