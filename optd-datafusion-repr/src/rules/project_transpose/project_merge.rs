use std::collections::HashMap;

use optd_core::rel_node::MaybeRelNode;
use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    ColumnRefExpr, ExprList, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;
use crate::rules::macros::define_rule;

use super::project_transpose_common::ProjectionMapping;

// Proj (Proj A) -> Proj A
// merges projections
define_rule!(
    ProjectMergeRule,
    apply_projection_merge,
    (Projection, (Projection, child, [exprs2]), [exprs1])
);

fn apply_projection_merge(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectMergeRulePicks {
        child,
        exprs1,
        exprs2,
    }: ProjectMergeRulePicks,
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child);
    let exprs1 = ExprList::ensures_interpret(exprs1);
    let exprs2 = ExprList::ensures_interpret(exprs2);

    let Some(mapping) = ProjectionMapping::build(&exprs1) else {
        return vec![];
    };

    let Some(res_exprs) = mapping.rewrite_projection(&exprs2, true) else {
        return vec![];
    };

    let node: LogicalProjection = LogicalProjection::new(child, res_exprs);

    vec![node.strip()]
}

// Proj child [identical columns] -> eliminate
define_rule!(
    EliminateProjectRule,
    apply_eliminate_project,
    (Projection, child, [expr])
);

fn apply_eliminate_project(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateProjectRulePicks { child, expr }: EliminateProjectRulePicks,
) -> Vec<MaybeRelNode<OptRelNodeTyp>> {
    let exprs = ExprList::ensures_interpret(expr);
    let child_columns = optimizer
        .get_property::<SchemaPropertyBuilder>(child.clone(), 0)
        .len();
    if child_columns != exprs.len() {
        return Vec::new();
    }
    for i in 0..exprs.len() {
        let child_expr = exprs.child(i);
        if child_expr.typ() == OptRelNodeTyp::ColumnRef {
            let child_expr = ColumnRefExpr::ensures_interpret(child_expr.strip());
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

    use optd_core::optimizer::Optimizer;

    use crate::{
        plan_nodes::{
            ColumnRefExpr, ExprList, LogicalProjection, LogicalScan, OptRelNode, OptRelNodeTyp,
        },
        rules::ProjectMergeRule,
        testing::new_test_optimizer,
    };

    #[test]
    fn proj_merge_basic() {
        // convert proj -> proj -> scan to proj -> scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectMergeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let top_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ]);

        let bot_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
        ]);

        let bot_proj = LogicalProjection::new(scan.into_plan_node(), bot_proj_exprs);
        let top_proj = LogicalProjection::new(bot_proj.into_plan_node(), top_proj_exprs);

        let plan = test_optimizer
            .optimize(top_proj.strip().unwrap_rel_node())
            .unwrap();

        let res_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ])
        .strip();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), res_proj_exprs);
        assert!(matches!(plan.child_rel(0).typ, OptRelNodeTyp::Scan));
    }

    #[test]
    fn proj_merge_adv() {
        // convert proj -> proj -> proj -> scan to proj -> scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectMergeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let proj_exprs_1 = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let proj_exprs_2 = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let proj_exprs_3 = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ]);

        let proj_1 = LogicalProjection::new(scan.into_plan_node(), proj_exprs_1);
        let proj_2 = LogicalProjection::new(proj_1.into_plan_node(), proj_exprs_2);
        let proj_3 = LogicalProjection::new(proj_2.into_plan_node(), proj_exprs_3);

        // needs to be called twice
        let plan = test_optimizer
            .optimize(proj_3.strip().unwrap_rel_node())
            .unwrap();
        let plan = test_optimizer.optimize(plan).unwrap();

        let res_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ])
        .strip();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), res_proj_exprs);
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Scan));
    }

    #[test]
    fn proj_merge_adv_2() {
        // convert proj -> proj -> proj -> proj -> scan to proj -> scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectMergeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let proj_exprs_1 = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let proj_exprs_2 = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let proj_exprs_3 = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ]);

        let proj_exprs_4 = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ]);

        let proj_1 = LogicalProjection::new(scan.into_plan_node(), proj_exprs_1);
        let proj_2 = LogicalProjection::new(proj_1.into_plan_node(), proj_exprs_2);
        let proj_3 = LogicalProjection::new(proj_2.into_plan_node(), proj_exprs_3);
        let proj_4 = LogicalProjection::new(proj_3.into_plan_node(), proj_exprs_4);

        // needs to be called three times
        let plan = test_optimizer
            .optimize(proj_4.strip().unwrap_rel_node())
            .unwrap();
        let plan = test_optimizer.optimize(plan).unwrap();
        let plan = test_optimizer.optimize(plan).unwrap();

        let res_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ])
        .strip();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), res_proj_exprs);
        assert!(matches!(plan.child_rel(0).typ, OptRelNodeTyp::Scan));
    }
}
