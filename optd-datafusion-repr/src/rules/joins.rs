use std::collections::HashMap;
use std::sync::Arc;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::{define_impl_rule, define_rule};
use crate::plan_nodes::{JoinType, OptRelNodeTyp};
use crate::properties::schema::SchemaPropertyBuilder;

define_rule!(
    JoinCommuteRule,
    apply_join_commute,
    (Join(JoinType::Inner), left, right, cond)
);

fn apply_join_commute(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinCommuteRulePicks { left, right, cond }: JoinCommuteRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let node = RelNode {
        typ: OptRelNodeTyp::Join(JoinType::Inner),
        children: vec![right.into(), left.into(), cond.into()],
        data: None,
    };
    vec![node]
}

define_rule!(
    JoinAssocRule,
    apply_join_assoc,
    (
        Join(JoinType::Inner),
        (Join(JoinType::Inner), a, b, cond1),
        c,
        cond2
    )
);

fn apply_join_assoc(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinAssocRulePicks {
        a,
        b,
        c,
        cond1,
        cond2,
    }: JoinAssocRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let node = RelNode {
        typ: OptRelNodeTyp::Join(JoinType::Inner),
        children: vec![
            a.into(),
            RelNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                children: vec![b.into(), c.into(), cond2.into()],
                data: None,
            }
            .into(),
            cond1.into(),
        ],
        data: None,
    };
    vec![node]
}

define_impl_rule!(
    HashJoinRule,
    apply_hash_join,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_hash_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    HashJoinRulePicks { left, right, cond }: HashJoinRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    println!("!!!");
    println!(
        "left: {}, schema={:?}",
        left,
        optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0)
    );
    println!(
        "right: {}, schema={:?}",
        right,
        optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0)
    );
    println!("cond: {}", cond);
    vec![]
}
