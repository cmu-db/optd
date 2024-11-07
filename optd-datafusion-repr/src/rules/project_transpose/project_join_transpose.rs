use std::vec;

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::RuleMatcher;

use super::project_transpose_common::ProjectionMapping;
use crate::plan_nodes::{
    ArcDfPlanNode, ColumnRefPred, DfNodeType, DfReprPlanNode, DfReprPredNode, JoinType, ListPred,
    LogicalJoin, LogicalProjection,
};
use crate::rules::macros::define_rule;
use crate::{OptimizerExt, Rule};

// (Proj A) join B -> (Proj (A join B))
define_rule!(
    ProjectionPullUpJoin,
    apply_projection_pull_up_join,
    (Join(JoinType::Inner), (Projection, left), right)
);

fn apply_projection_pull_up_join(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let join = LogicalJoin::from_plan_node(binding).unwrap();
    let projection = LogicalProjection::from_plan_node(join.left().unwrap_plan_node()).unwrap();
    let left = projection.child();
    let right = join.right();
    let list = projection.exprs();
    let cond = join.cond();

    let projection = LogicalProjection::new_unchecked(left.clone(), list.clone());

    let Some(mapping) = ProjectionMapping::build(&projection.exprs()) else {
        return vec![];
    };

    // TODO(chi): support capture projection node.
    let left_schema = optimizer.get_schema_of(left.clone());
    let right_schema = optimizer.get_schema_of(right.clone());
    let mut new_projection_exprs = list.to_vec();
    for i in 0..right_schema.len() {
        let col = ColumnRefPred::new(i + left_schema.len()).into_pred_node();
        new_projection_exprs.push(col);
    }
    let node = LogicalProjection::new(
        LogicalJoin::new_unchecked(
            left,
            right,
            mapping.rewrite_join_cond(cond, left_schema.len()),
            JoinType::Inner,
        )
        .into_plan_node(),
        ListPred::new(new_projection_exprs),
    );
    vec![node.into_plan_node().into()]
}
