// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::vec;

use itertools::Itertools;
use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;
use optd_core::rules::{Rule, RuleMatcher};

use super::project_transpose_common::ProjectionMapping;
use crate::plan_nodes::{
    ArcDfPlanNode, DfNodeType, DfReprPlanNode, DfReprPredNode, ListPred, LogicalFilter,
    LogicalProjection, PredExt,
};
use crate::rules::macros::define_rule;

fn merge_exprs(first: ListPred, second: ListPred) -> ListPred {
    let mut res_vec = first.to_vec();
    res_vec.extend(second.to_vec());
    ListPred::new(res_vec)
}

define_rule!(
    ProjectFilterTransposeRule,
    apply_projection_filter_transpose,
    (Projection, (Filter, child))
);

/// pushes projections through filters
/// adds a projection node after a filter node
/// only keeping necessary columns (proj node exprs + filter col exprs)
fn apply_projection_filter_transpose(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let projection = LogicalProjection::from_plan_node(binding).unwrap();
    let filter = LogicalFilter::from_plan_node(projection.child().unwrap_plan_node()).unwrap();
    let child = filter.child();
    let exprs = projection.exprs();
    let cond = filter.cond();

    // get columns out of cond
    let exprs_vec = exprs.to_vec();
    let cond_col_refs = cond
        .get_column_refs()
        .into_iter()
        .map(|x| x.into_pred_node())
        .collect_vec();
    let mut dedup_cond_col_refs = Vec::new();

    for col_ref in &cond_col_refs {
        if !exprs_vec.contains(col_ref) {
            dedup_cond_col_refs.push(col_ref.clone());
        };
    }

    let bottom_proj_exprs = merge_exprs(exprs.clone(), ListPred::new(dedup_cond_col_refs.clone()));
    let Some(mapping) = ProjectionMapping::build(&bottom_proj_exprs) else {
        return vec![];
    };

    let new_filter_cond = mapping.rewrite_filter_cond(cond, true);
    let bottom_proj_node = LogicalProjection::new_unchecked(child, bottom_proj_exprs);
    let new_filter_node = LogicalFilter::new(bottom_proj_node.into_plan_node(), new_filter_cond);

    if dedup_cond_col_refs.is_empty() {
        // can push proj past filter and remove top proj node
        return vec![new_filter_node.into_plan_node().into()];
    }

    // have column ref expressions of cond cols
    // bottom-most projection will have proj cols + filter cols as a set
    let Some(top_proj_exprs) = mapping.rewrite_projection(&exprs, false) else {
        return vec![];
    };
    let top_proj_node = LogicalProjection::new(new_filter_node.into_plan_node(), top_proj_exprs);
    vec![top_proj_node.into_plan_node().into()]
}

define_rule!(
    FilterProjectTransposeRule,
    apply_filter_project_transpose,
    (Filter, (Projection, child))
);

/// Datafusion only pushes filter past project when the project does not contain
/// volatile (i.e. non-deterministic) expressions that are present in the filter
/// Calcite only checks if the projection contains a windowing calculation
/// We check neither of those things and do it always (which may be wrong)
fn apply_filter_project_transpose(
    _optimizer: &impl Optimizer<DfNodeType>,
    binding: ArcDfPlanNode,
) -> Vec<PlanNodeOrGroup<DfNodeType>> {
    let filter = LogicalFilter::from_plan_node(binding).unwrap();
    let proj = LogicalProjection::from_plan_node(filter.child().unwrap_plan_node()).unwrap();
    let child = proj.child();
    let exprs = proj.exprs();
    let cond = filter.cond();

    let proj_col_map = ProjectionMapping::build(&exprs).unwrap();
    let rewritten_cond = proj_col_map.rewrite_filter_cond(cond, false);

    let new_filter_node = LogicalFilter::new_unchecked(child, rewritten_cond);
    let new_proj = LogicalProjection::new(new_filter_node.into_plan_node(), exprs);
    vec![new_proj.into_plan_node().into()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimizer::Optimizer;

    use super::*;
    use crate::plan_nodes::{
        BinOpPred, BinOpType, ColumnRefPred, ConstantPred, LogOpPred, LogOpType, LogicalScan,
    };
    use crate::testing::new_test_optimizer;

    // ProjectFilterTransposeRule Tests
    #[test]
    fn push_proj_past_filter_basic_1() {
        // convert proj -> filter -> scan to filter -> proj -> scan
        // happens when all filter expr col refs are in proj exprs
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let filter_expr = BinOpPred::new(
            ColumnRefPred::new(0).into_pred_node(),
            ConstantPred::int32(5).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);

        let proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(0).into_pred_node(),
        ]);

        let proj = LogicalProjection::new(filter.into_plan_node(), proj_exprs.clone());

        let plan = test_optimizer.optimize(proj.into_plan_node()).unwrap();

        let res_filter_expr = BinOpPred::new(
            ColumnRefPred::new(1).into_pred_node(),
            ConstantPred::int32(5).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        assert_eq!(plan.predicate(0), res_filter_expr);
        assert_eq!(plan.typ, DfNodeType::Filter);
        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Projection));
        assert_eq!(plan.child_rel(0).predicate(0), proj_exprs.into_pred_node());
        assert!(matches!(
            plan.child_rel(0).child_rel(0).typ,
            DfNodeType::Scan
        ));
    }

    #[test]
    fn push_proj_past_filter_basic_2() {
        // convert proj -> filter -> scan to filter -> proj -> scan
        // happens when all filter expr col refs are NOT in proj exprs

        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("region".into());

        let filter_expr = BinOpPred::new(
            ColumnRefPred::new(2).into_pred_node(),
            ConstantPred::int32(5).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);

        let proj_exprs = ListPred::new(vec![ColumnRefPred::new(1).into_pred_node()]);

        let res_filter_expr = BinOpPred::new(
            ColumnRefPred::new(1).into_pred_node(),
            ConstantPred::int32(5).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        let res_top_proj_exprs =
            ListPred::new(vec![ColumnRefPred::new(0).into_pred_node()]).into_pred_node();

        let res_bot_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
        ])
        .into_pred_node();

        let proj = LogicalProjection::new(filter.into_plan_node(), proj_exprs);

        let plan = test_optimizer.optimize(proj.into_plan_node()).unwrap();

        assert_eq!(plan.typ, DfNodeType::Projection);
        assert_eq!(plan.predicate(0), res_top_proj_exprs);

        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Filter));
        assert_eq!(plan.child_rel(0).predicate(0), res_filter_expr);

        assert!(matches!(
            plan.child_rel(0).child_rel(0).typ,
            DfNodeType::Projection
        ));
        assert_eq!(
            plan.child_rel(0).child_rel(0).predicate(0),
            res_bot_proj_exprs
        );

        assert!(matches!(
            plan.child_rel(0).child_rel(0).child_rel(0).typ,
            DfNodeType::Scan
        ));
    }

    #[test]
    fn push_proj_past_filter_adv_1() {
        let mut test_optimizer: optd_core::heuristics::HeuristicsOptimizer<DfNodeType> =
            new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    ColumnRefPred::new(5).into_pred_node(),
                    ConstantPred::int32(3).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    ConstantPred::int32(6).into_pred_node(),
                    ColumnRefPred::new(0).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        )
        .into_pred_node();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);
        let proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(4).into_pred_node(),
            ColumnRefPred::new(5).into_pred_node(),
            ColumnRefPred::new(7).into_pred_node(),
        ]);

        let proj =
            LogicalProjection::new(filter.into_plan_node(), proj_exprs.clone()).into_plan_node();

        let plan = test_optimizer.optimize(proj).unwrap();

        let res_filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    ColumnRefPred::new(2).into_pred_node(),
                    ConstantPred::int32(3).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    ConstantPred::int32(6).into_pred_node(),
                    ColumnRefPred::new(0).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        )
        .into_pred_node();

        assert!(matches!(plan.typ, DfNodeType::Filter));
        assert_eq!(plan.predicate(0), res_filter_expr);

        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Projection));
        assert_eq!(plan.child_rel(0).predicate(0), proj_exprs.into_pred_node());
    }

    #[test]
    fn push_proj_past_filter_adv_2() {
        let mut test_optimizer: optd_core::heuristics::HeuristicsOptimizer<DfNodeType> =
            new_test_optimizer(Arc::new(ProjectFilterTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());

        let filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    ColumnRefPred::new(5).into_pred_node(),
                    ConstantPred::int32(3).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    ConstantPred::int32(6).into_pred_node(),
                    ColumnRefPred::new(2).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        )
        .into_pred_node();

        let filter = LogicalFilter::new(scan.into_plan_node(), filter_expr);
        let proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(4).into_pred_node(),
            ColumnRefPred::new(5).into_pred_node(),
            ColumnRefPred::new(7).into_pred_node(),
        ]);

        let proj =
            LogicalProjection::new(filter.into_plan_node(), proj_exprs.clone()).into_plan_node();

        let plan = test_optimizer.optimize(proj).unwrap();

        let res_filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    ColumnRefPred::new(2).into_pred_node(),
                    ConstantPred::int32(3).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    ConstantPred::int32(6).into_pred_node(),
                    ColumnRefPred::new(4).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        )
        .into_pred_node();

        let top_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(1).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
            ColumnRefPred::new(3).into_pred_node(),
        ])
        .into_pred_node();

        let bot_proj_exprs = ListPred::new(vec![
            ColumnRefPred::new(0).into_pred_node(),
            ColumnRefPred::new(4).into_pred_node(),
            ColumnRefPred::new(5).into_pred_node(),
            ColumnRefPred::new(7).into_pred_node(),
            ColumnRefPred::new(2).into_pred_node(),
        ])
        .into_pred_node();

        assert!(matches!(plan.typ, DfNodeType::Projection));
        assert_eq!(plan.predicate(0), top_proj_exprs);

        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Filter));
        assert_eq!(plan.child_rel(0).predicate(0), res_filter_expr);

        assert!(matches!(
            plan.child_rel(0).child_rel(0).typ,
            DfNodeType::Projection
        ));
        assert_eq!(plan.child_rel(0).child_rel(0).predicate(0), bot_proj_exprs);
    }

    // FilterProjectTransposeRule Tests
    #[test]
    fn push_filter_past_proj_basic() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterProjectTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(
            scan.into_plan_node(),
            ListPred::new(vec![ColumnRefPred::new(0).into_pred_node()]),
        );

        let filter_expr = BinOpPred::new(
            ColumnRefPred::new(0).into_pred_node(),
            ConstantPred::int32(5).into_pred_node(),
            BinOpType::Eq,
        )
        .into_pred_node();

        let filter = LogicalFilter::new(proj.into_plan_node(), filter_expr);
        let plan = test_optimizer.optimize(filter.into_plan_node()).unwrap();

        assert_eq!(plan.typ, DfNodeType::Projection);
        assert!(matches!(plan.child_rel(0).typ, DfNodeType::Filter));
    }

    #[test]
    fn push_filter_past_proj_adv() {
        let mut test_optimizer = new_test_optimizer(Arc::new(FilterProjectTransposeRule::new()));

        let scan = LogicalScan::new("customer".into());
        let proj = LogicalProjection::new(
            scan.into_plan_node(),
            ListPred::new(vec![
                ColumnRefPred::new(0).into_pred_node(),
                ColumnRefPred::new(4).into_pred_node(),
                ColumnRefPred::new(5).into_pred_node(),
                ColumnRefPred::new(7).into_pred_node(),
            ]),
        );

        let filter_expr = LogOpPred::new(
            LogOpType::And,
            vec![
                BinOpPred::new(
                    // This one should be pushed to the left child
                    ColumnRefPred::new(1).into_pred_node(),
                    ConstantPred::int32(5).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
                BinOpPred::new(
                    // This one should be pushed to the right child
                    ColumnRefPred::new(3).into_pred_node(),
                    ConstantPred::int32(6).into_pred_node(),
                    BinOpType::Eq,
                )
                .into_pred_node(),
            ],
        );

        let filter = LogicalFilter::new(proj.into_plan_node(), filter_expr.into_pred_node());

        let plan = test_optimizer.optimize(filter.into_plan_node()).unwrap();

        assert!(matches!(plan.typ, DfNodeType::Projection));
        let plan_filter = LogicalFilter::from_plan_node(plan.child_rel(0)).unwrap();
        assert!(matches!(plan_filter.0.typ, DfNodeType::Filter));
        let plan_filter_expr = LogOpPred::from_pred_node(plan_filter.cond()).unwrap();
        assert!(matches!(plan_filter_expr.op_type(), LogOpType::And));
        let op_0 = BinOpPred::from_pred_node(plan_filter_expr.children()[0].clone()).unwrap();
        let col_0 = ColumnRefPred::from_pred_node(op_0.left_child().clone()).unwrap();
        assert_eq!(col_0.index(), 4);
        let op_1 = BinOpPred::from_pred_node(plan_filter_expr.children()[1].clone()).unwrap();
        let col_1 = ColumnRefPred::from_pred_node(op_1.left_child().clone()).unwrap();
        assert_eq!(col_1.index(), 7);
    }
}
