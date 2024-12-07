// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::define_rule;
use crate::plan_nodes::{
    ArcDfPlanNode, ConstantPred, ConstantType, DfNodeType, DfPredType, DfReprPlanNode,
    DfReprPredNode, LogicalEmptyRelation, LogicalLimit,
};
use crate::OptimizerExt;

define_rule!(EliminateLimitRule, apply_eliminate_limit, (Limit, child));

/// Transformations:
///     - Limit with skip 0 and no fetch -> Eliminate from the tree
///     - Limit with limit 0 -> EmptyRelation
fn apply_eliminate_limit(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let limit = LogicalLimit::from_plan_node(binding).unwrap();
    let skip = limit.skip();
    let fetch = limit.fetch();
    let child = limit.child();
    if let DfPredType::Constant(ConstantType::Int64) = skip.typ {
        if let DfPredType::Constant(ConstantType::Int64) = fetch.typ {
            let skip_val = ConstantPred::from_pred_node(skip).unwrap().value().as_i64();

            let fetch_val = ConstantPred::from_pred_node(fetch)
                .unwrap()
                .value()
                .as_i64();

            // Bad convention to have u64 max represent None
            let fetch_is_none = fetch_val == i64::MAX;

            let schema = optimizer.get_schema_of(child.clone());
            if fetch_is_none && skip_val == 0 {
                return vec![child];
            } else if fetch_val == 0 {
                let node = LogicalEmptyRelation::new(false, schema);
                return vec![node.into_plan_node().into()];
            }
        }
    }
    vec![]
}
