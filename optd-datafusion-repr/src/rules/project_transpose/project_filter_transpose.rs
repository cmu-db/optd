use std::collections::HashMap;
use std::vec;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use super::project_transpose_common::{ProjectionMapping, merge_exprs};
use crate::plan_nodes::{
    Expr, ExprList, LogicalFilter, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::rules::macros::define_rule;

define_rule!(
    ProjectFilterTransposeRule,
    apply_projection_filter_transpose,
    (Projection, (Filter, child, [cond]), [exprs])
);

/// pushes projections through filters
/// adds a projection node after a filter node
/// only keeping necessary columns (proj node exprs + filter col exprs)
fn apply_projection_filter_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectFilterTransposeRulePicks { child, cond, exprs }: ProjectFilterTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // get columns out of cond
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();
    let exprs_vec = exprs.clone().to_vec();
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let cond_col_refs = cond_as_expr.get_column_refs();
    let mut dedup_cond_col_refs = Vec::new();

    for col_ref in &cond_col_refs {
        if !exprs_vec.contains(col_ref) {
            dedup_cond_col_refs.push(col_ref.clone());
        };
    }

    let dedup_cond_col_refs = ExprList::new(dedup_cond_col_refs);

    let bottom_proj_exprs: ExprList = merge_exprs(exprs.clone(), dedup_cond_col_refs.clone());
    let Some(mapping) = ProjectionMapping::build(&bottom_proj_exprs) else {
        return vec![];
    };

    let child: PlanNode = PlanNode::from_group(child.into());
    let new_filter_cond: Expr = mapping.rewrite_filter_cond(cond_as_expr.clone(), true);
    let bottom_proj_node = LogicalProjection::new(child, bottom_proj_exprs);
    let new_filter_node = LogicalFilter::new(bottom_proj_node.into_plan_node(), new_filter_cond);

    if dedup_cond_col_refs.is_empty() {
        // can push proj past filter and remove top proj node
        return vec![new_filter_node.into_rel_node().as_ref().clone()];
    }

    // have column ref expressions of cond cols
    // bottom-most projection will have proj cols + filter cols as a set
    let Some(top_proj_exprs) = mapping.rewrite_projection(&exprs, false) else {
        return vec![];
    };
    let top_proj_node = LogicalProjection::new(new_filter_node.into_plan_node(), top_proj_exprs);
    vec![top_proj_node.into_rel_node().as_ref().clone()]
}

define_rule!(
    FilterProjectTransposeRule,
    apply_filter_project_transpose,
    (Filter, (Projection, child, [exprs]), [cond])
);

/// Datafusion only pushes filter past project when the project does not contain
/// volatile (i.e. non-deterministic) expressions that are present in the filter
/// Calcite only checks if the projection contains a windowing calculation
/// We check neither of those things and do it always (which may be wrong)
fn apply_filter_project_transpose(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterProjectTransposeRulePicks { child, exprs, cond }: FilterProjectTransposeRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child = PlanNode::from_group(child.into());
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap();

    let proj_col_map = ProjectionMapping::build(&exprs).unwrap();
    let rewritten_cond = proj_col_map.rewrite_filter_cond(cond_as_expr.clone(), false);

    let new_filter_node = LogicalFilter::new(child, rewritten_cond);
    let new_proj = LogicalProjection::new(new_filter_node.into_plan_node(), exprs);
    vec![new_proj.into_rel_node().as_ref().clone()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimizer::Optimizer;

    use crate::{
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ExprList, LogOpExpr, LogOpType,
            LogicalFilter, LogicalProjection, LogicalScan, OptRelNode, OptRelNodeTyp,
        },
        rules::{FilterProjectTransposeRule, ProjectFilterTransposeRule},
        testing::new_test_optimizer,
    };

    // ProjectFilterTransposeRule Tests
    #[test]
    fn push_proj_past_filter_basic_1() {
        // convert proj -> filter -> scan to filter -> proj -> scan
        // happens when all filter expr col refs are in proj exprs
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);

        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ]);

        let proj = LogicalProjection::new(filter.into_plan_node(), proj_exprs.clone());

        let plan = test_optimizer.optimize(proj.into_rel_node()).unwrap();

        let res_filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(1).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr()
        .into_rel_node();

        assert_eq!(plan.child(1), res_filter_expr);
        assert_eq!(plan.typ, OptRelNodeTyp::Filter);
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Projection));
        assert_eq!(plan.child(0).child(1), proj_exprs.into_rel_node());
        assert!(matches!(plan.child(0).child(0).typ, OptRelNodeTyp::Scan));
    }

    #[test]
    fn push_proj_past_filter_basic_2() {
        // convert proj -> filter -> scan to filter -> proj -> scan
        // happens when all filter expr col refs are NOT in proj exprs

        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("region".into());

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(2).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);

        let proj_exprs = ExprList::new(vec![ColumnRefExpr::new(1).into_expr()]);

        let res_filter_expr: Arc<optd_core::rel_node::RelNode<OptRelNodeTyp>> = BinOpExpr::new(
            ColumnRefExpr::new(1).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr()
        .into_rel_node();

        let res_top_proj_exprs: Arc<optd_core::rel_node::RelNode<OptRelNodeTyp>> =
            ExprList::new(vec![ColumnRefExpr::new(0).into_expr()]).into_rel_node();

        let res_bot_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ])
        .into_rel_node();

        let proj = LogicalProjection::new(filter.into_plan_node(), proj_exprs);

        let plan = test_optimizer.optimize(proj.into_rel_node()).unwrap();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), res_top_proj_exprs);

        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
        assert_eq!(plan.child(0).child(1), res_filter_expr);

        assert!(matches!(
            plan.child(0).child(0).typ,
            OptRelNodeTyp::Projection
        ));
        assert_eq!(plan.child(0).child(0).child(1), res_bot_proj_exprs);

        assert!(matches!(
            plan.child(0).child(0).child(0).typ,
            OptRelNodeTyp::Scan
        ));
    }

    #[test]
    fn push_proj_past_filter_adv_1() {
        let mut test_optimizer: optd_core::heuristics::HeuristicsOptimizer<OptRelNodeTyp> =
            new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    ColumnRefExpr::new(5).into_expr(),
                    ConstantExpr::int32(3).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    ConstantExpr::int32(6).into_expr(),
                    ColumnRefExpr::new(0).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        )
        .into_expr();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);
        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(5).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
        ]);

        let proj =
            LogicalProjection::new(filter.into_plan_node(), proj_exprs.clone()).into_rel_node();

        let plan = test_optimizer.optimize(proj).unwrap();

        let res_filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    ColumnRefExpr::new(2).into_expr(),
                    ConstantExpr::int32(3).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    ConstantExpr::int32(6).into_expr(),
                    ColumnRefExpr::new(0).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        )
        .into_expr()
        .into_rel_node();

        assert!(matches!(plan.typ, OptRelNodeTyp::Filter));
        assert_eq!(plan.child(1), res_filter_expr);

        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Projection));
        assert_eq!(plan.child(0).child(1), proj_exprs.into_rel_node());
    }

    #[test]
    fn push_proj_past_filter_adv_2() {
        let mut test_optimizer: optd_core::heuristics::HeuristicsOptimizer<OptRelNodeTyp> =
            new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    ColumnRefExpr::new(5).into_expr(),
                    ConstantExpr::int32(3).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    ConstantExpr::int32(6).into_expr(),
                    ColumnRefExpr::new(2).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        )
        .into_expr();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);
        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(5).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
        ]);

        let proj =
            LogicalProjection::new(filter.into_plan_node(), proj_exprs.clone()).into_rel_node();

        let plan = test_optimizer.optimize(proj).unwrap();

        let res_filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    ColumnRefExpr::new(2).into_expr(),
                    ConstantExpr::int32(3).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    ConstantExpr::int32(6).into_expr(),
                    ColumnRefExpr::new(4).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        )
        .into_expr()
        .into_rel_node();

        let top_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ])
        .into_rel_node();

        let bot_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(5).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ])
        .into_rel_node();

        assert!(matches!(plan.typ, OptRelNodeTyp::Projection));
        assert_eq!(plan.child(1), top_proj_exprs);

        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
        assert_eq!(plan.child(0).child(1), res_filter_expr);

        assert!(matches!(
            plan.child(0).child(0).typ,
            OptRelNodeTyp::Projection
        ));
        assert_eq!(plan.child(0).child(0).child(1), bot_proj_exprs);
    }

    // FilterProjectTransposeRule Tests
    #[test]
    fn push_filter_past_proj_basic() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterProjectTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(
            scan.into_plan_node(),
            ExprList::new(vec![ColumnRefExpr::new(0).into_expr()]),
        );

        let filter_expr = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ConstantExpr::int32(5).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let filter = LogicalFilter::new(proj.into_plan_node(), filter_expr);
        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert!(matches!(plan.child(0).typ, OptRelNodeTyp::Filter));
    }

    #[test]
    fn push_filter_past_proj_adv() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterProjectTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(
            scan.into_plan_node(),
            ExprList::new(vec![
                ColumnRefExpr::new(0).into_expr(),
                ColumnRefExpr::new(4).into_expr(),
                ColumnRefExpr::new(5).into_expr(),
                ColumnRefExpr::new(7).into_expr(),
            ]),
        );

        let filter_expr = LogOpExpr::new(
            LogOpType::And,
            ExprList::new(vec![
                BinOpExpr::new(
                    // This one should be pushed to the left child
                    ColumnRefExpr::new(1).into_expr(),
                    ConstantExpr::int32(5).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
                BinOpExpr::new(
                    // This one should be pushed to the right child
                    ColumnRefExpr::new(3).into_expr(),
                    ConstantExpr::int32(6).into_expr(),
                    BinOpType::Eq,
                )
                .into_expr(),
            ]),
        );

        let filter = LogicalFilter::new(proj.into_plan_node(), filter_expr.into_expr());

        let plan = test_optimizer.optimize(filter.into_rel_node()).unwrap();

        assert!(matches!(plan.typ, OptRelNodeTyp::Projection));
        let plan_filter = LogicalFilter::from_rel_node(plan.child(0)).unwrap();
        assert!(matches!(plan_filter.0.typ(), OptRelNodeTyp::Filter));
        let plan_filter_expr =
            LogOpExpr::from_rel_node(plan_filter.cond().into_rel_node()).unwrap();
        assert!(matches!(plan_filter_expr.op_type(), LogOpType::And));
        let op_0 = BinOpExpr::from_rel_node(plan_filter_expr.children()[0].clone().into_rel_node())
            .unwrap();
        let col_0 =
            ColumnRefExpr::from_rel_node(op_0.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_0.index(), 4);
        let op_1 = BinOpExpr::from_rel_node(plan_filter_expr.children()[1].clone().into_rel_node())
            .unwrap();
        let col_1 =
            ColumnRefExpr::from_rel_node(op_1.left_child().clone().into_rel_node()).unwrap();
        assert_eq!(col_1.index(), 7);
    }
}
