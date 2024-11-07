use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::project_transpose_common::ProjectionMapping;
use crate::plan_nodes::{
    ArcDfPlanNode, ColumnRefPred, DfNodeType, DfReprPlanNode, DfReprPredNode, LogicalProjection,
};
use crate::rules::macros::define_rule;
use crate::OptimizerExt;

// Proj (Proj A) -> Proj A
// merges projections
define_rule!(
    ProjectMergeRule,
    apply_projection_merge,
    (Projection, (Projection, child))
);

fn apply_projection_merge(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let proj1 = LogicalProjection::from_plan_node(binding).unwrap();
    let proj2 = LogicalProjection::from_plan_node(proj1.child().unwrap_plan_node()).unwrap();
    let child = proj2.child();
    let exprs1 = proj1.exprs();
    let exprs2 = proj2.exprs();

    let Some(mapping) = ProjectionMapping::build(&exprs1) else {
        return vec![];
    };

    let Some(res_exprs) = mapping.rewrite_projection(&exprs2, true) else {
        return vec![];
    };

    let node = LogicalProjection::new_unchecked(child, res_exprs);

    vec![node.into_plan_node().into()]
}

// Proj child [identical columns] -> eliminate
define_rule!(
    EliminateProjectRule,
    apply_eliminate_project,
    (Projection, child)
);

fn apply_eliminate_project(
    optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let proj = LogicalProjection::from_plan_node(binding).unwrap();
    let child = proj.child();
    let exprs = proj.exprs();
    let child_schema = optimizer.get_schema_of(child.clone());
    if child_schema.len() != exprs.len() {
        return Vec::new();
    }
    for i in 0..exprs.len() {
        let child_expr = exprs.child(i);
        if let Some(child_expr) = ColumnRefPred::from_pred_node(child_expr) {
            if child_expr.index() != i {
                return Vec::new();
            }
        } else {
            return Vec::new();
        }
    }
    vec![child]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::plan_nodes::{ListPred, LogicalScan};
    use crate::testing::new_test_optimizer;

    #[test]
    fn proj_merge_basic() {
        // convert proj -> proj -> scan to proj -> scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectMergeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let top_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
        ]);

        let bot_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(4).into_pred_node(),
        ]);

        let bot_proj = LogicalProjection::new(scan.into_plan_node(), bot_proj_exprs);
        let top_proj = LogicalProjection::new(bot_proj.into_plan_node(), top_proj_exprs);

        let plan = test_optimizer.optimize(top_proj.into_plan_node()).unwrap();

        let res_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(4).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
        ])
        .into_pred_node();

        assert_eq!(plan.typ, DfNodeType::Projection);
        assert_eq!(plan.predicate(0), res_proj_exprs);
        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Scan));
    }

    #[test]
    fn proj_merge_adv() {
        // convert proj -> proj -> proj -> scan to proj -> scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectMergeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let proj_exprs_1 = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(4).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ]);

        let proj_exprs_2 = ListPred::new(vec![
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ]);

        let proj_exprs_3 = ListPred::new(vec![
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
        ]);

        let proj_1 = LogicalProjection::new(scan.into_plan_node(), proj_exprs_1);
        let proj_2 = LogicalProjection::new(proj_1.into_plan_node(), proj_exprs_2);
        let proj_3 = LogicalProjection::new(proj_2.into_plan_node(), proj_exprs_3);

        // needs to be called twice
        let plan = test_optimizer.optimize(proj_3.into_plan_node()).unwrap();
        let plan = test_optimizer.optimize(plan).unwrap();

        let res_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ])
        .into_pred_node();

        assert_eq!(plan.typ, DfNodeType::Projection);
        assert_eq!(plan.predicate(0), res_proj_exprs);
        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Scan));
    }

    #[test]
    fn proj_merge_adv_2() {
        // convert proj -> proj -> proj -> proj -> scan to proj -> scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectMergeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let proj_exprs_1 = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(4).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ]);

        let proj_exprs_2 = ListPred::new(vec![
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ]);

        let proj_exprs_3 = ListPred::new(vec![
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
        ]);

        let proj_exprs_4 = ListPred::new(vec![
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
        ]);

        let proj_1 = LogicalProjection::new(scan.into_plan_node(), proj_exprs_1);
        let proj_2 = LogicalProjection::new(proj_1.into_plan_node(), proj_exprs_2);
        let proj_3 = LogicalProjection::new(proj_2.into_plan_node(), proj_exprs_3);
        let proj_4 = LogicalProjection::new(proj_3.into_plan_node(), proj_exprs_4);

        // needs to be called three times
        let plan = test_optimizer.optimize(proj_4.into_plan_node()).unwrap();
        let plan = test_optimizer.optimize(plan).unwrap();
        let plan = test_optimizer.optimize(plan).unwrap();

        let res_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ])
        .into_pred_node();

        assert_eq!(plan.typ, DfNodeType::Projection);
        assert_eq!(plan.predicate(0), res_proj_exprs);
        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Scan));
    }
}
