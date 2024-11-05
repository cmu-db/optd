use optd_core::rel_node::MaybeRelNode;
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::plan_nodes::{
    ConstantExpr, ConstantType, LogicalEmptyRelation, OptRelNode, OptRelNodeTyp,
};

use super::macros::define_rule;
use crate::properties::schema::SchemaPropertyBuilder;

define_rule!(
    EliminateLimitRule,
    apply_eliminate_limit,
    (Limit, child, [skip], [fetch])
);

/// Transformations:
///     - Limit with skip 0 and no fetch -> Eliminate from the tree
///     - Limit with limit 0 -> EmptyRelation
fn apply_eliminate_limit(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateLimitRulePicks { child, skip, fetch }: EliminateLimitRulePicks,
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::Constant(ConstantType::UInt64) = skip.unwrap_typ() {
        if let OptRelNodeTyp::Constant(ConstantType::UInt64) = fetch.unwrap_typ() {
            let skip_val = ConstantExpr::ensures_interpret(skip).value().as_u64();

            let fetch_val = ConstantExpr::ensures_interpret(fetch).value().as_u64();

            // Bad convention to have u64 max represent None
            let fetch_is_none = fetch_val == u64::MAX;

            let schema = optimizer.get_property::<SchemaPropertyBuilder>(child, 0);

            if fetch_is_none && skip_val == 0 {
                return vec![child];
            } else if fetch_val == 0 {
                let node = LogicalEmptyRelation::new(false, schema);
                return vec![node.strip()];
            }
        }
    }
    vec![]
}
