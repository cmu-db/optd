use crate::HashMap;
use crate::Rule;

use optd_core::rules::RuleMatcher;
use std::sync::Arc;
use std::vec;

use crate::rules::macros::define_rule;
use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;

use super::project_transpose_common::{ProjectionMapping, merge_exprs, split_exprs};
use crate::plan_nodes::{
    ColumnRefExpr, Expr, ExprList, JoinType, LogicalJoin, LogicalProjection, 
    OptRelNode, OptRelNodeTyp, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;

// (Proj A) join B -> (Proj (A join B))
// TODO: rule currently doesn't work under current 
// cascades + heuristic rule wrapper infrastructure
// correctness verified using unit tests in current file
// and via comparisons against CockroachDB & Calcite
define_rule!(
    ProjectionPullUpJoin,
    apply_projection_pull_up_join,
    (
        Join(JoinType::Inner),
        (Projection, left, [list]),
        right,
        [cond]
    )
);

fn apply_projection_pull_up_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectionPullUpJoinPicks {
        left,
        right,
        list,
        cond,
    }: ProjectionPullUpJoinPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let left = Arc::new(left.clone());
    let right = Arc::new(right.clone());

    let list = ExprList::from_rel_node(Arc::new(list)).unwrap();

    let projection = LogicalProjection::new(PlanNode::from_group(left.clone()), list.clone());

    let Some(mapping) = ProjectionMapping::build(&projection.exprs()) else {
        return vec![];
    };

    // TODO(chi): support capture projection node.
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(right.clone(), 0);
    let mut new_projection_exprs = list.to_vec();
    for i in 0..right_schema.len() {
        let col: Expr = ColumnRefExpr::new(i + left_schema.len()).into_expr();
        new_projection_exprs.push(col);
    }
    let node = LogicalProjection::new(
        LogicalJoin::new(
            PlanNode::from_group(left),
            PlanNode::from_group(right),
            mapping.rewrite_join_cond(
                Expr::from_rel_node(Arc::new(cond)).unwrap(),
                left_schema.len(),
                false,
                true,
                0
            ),
            JoinType::Inner,
        )
        .into_plan_node(),
        ExprList::new(new_projection_exprs),
    );
    vec![node.into_rel_node().as_ref().clone()]    
}

// most general: (Proj (A join B) -> Proj ((Proj A) join (Proj B))
// ideal: (Proj (A join B) -> (Proj A) join (Proj B)
define_rule!(
    ProjectionPushDownJoin,
    apply_projection_push_down_join,
    (
        Projection,
        (Join(JoinType::Inner), left, right, [cond]),
        [exprs]
    )
);

fn apply_projection_push_down_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectionPushDownJoinPicks {
        left,
        right,
        cond,
        exprs,
    }: ProjectionPushDownJoinPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let left = Arc::new(left.clone());
    let right = Arc::new(right.clone());

    let exprs = ExprList::from_rel_node(Arc::new(exprs)).unwrap();
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
    let tot_exprs = merge_exprs(exprs.clone(), dedup_cond_col_refs.clone());

    // split exprs into exprs based on left + right children
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
    let left_schema_len = left_schema.len();

    let (left_exprs, right_exprs, is_left_right_ordered) = split_exprs(tot_exprs, left_schema_len);
    let new_left_schema_len = left_exprs.len();

    let Some(left_exprs_mapping) = ProjectionMapping::build(&left_exprs) else {
        return vec![];
    };

    let Some(right_exprs_mapping) = ProjectionMapping::build(&right_exprs) else {
        return vec![];
    };

    // update join cond based on new left + right child projection nodes
    let new_join_cond: Expr = left_exprs_mapping.rewrite_join_cond(cond_as_expr.clone(), left_schema_len, true, true, 0);
    let new_join_cond: Expr = right_exprs_mapping.rewrite_join_cond(new_join_cond.clone(), left_schema_len, true, false, new_left_schema_len);
    
    let new_left_child = LogicalProjection::new(
        PlanNode::from_group(left),
        left_exprs
    )
    .into_plan_node();

    let new_right_child = LogicalProjection::new(
        PlanNode::from_group(right),
        right_exprs
    )
    .into_plan_node();

    let new_join_node = LogicalJoin::new(
        new_left_child,
        new_right_child,
        new_join_cond,
        JoinType::Inner,
    )
    .into_plan_node();

    if dedup_cond_col_refs.is_empty() && is_left_right_ordered {
        // don't need top projection node
        return vec![new_join_node.into_rel_node().as_ref().clone()];
    }

    // update top projection node based on new left + right child projection nodes
    let mut top_proj_exprs = vec![];
    let mut left_col_idx = 0;
    let mut right_col_idx = new_left_schema_len;
    for i in 0..exprs.len() {
        let old_col_ref = ColumnRefExpr::from_rel_node(exprs_vec[i].clone().into_rel_node()).unwrap();
        if old_col_ref.index() < left_schema_len {
            top_proj_exprs.push(ColumnRefExpr::new(left_col_idx).into_expr());
            left_col_idx += 1;
        } else {
            top_proj_exprs.push(ColumnRefExpr::new(right_col_idx).into_expr());
            right_col_idx += 1;
        }
    }
    let top_proj_exprs = ExprList::new(top_proj_exprs);

    let new_top_node = LogicalProjection::new(
        new_join_node,
        top_proj_exprs,
    );
    vec![new_top_node.into_rel_node().as_ref().clone()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimizer::Optimizer;

    use crate::{
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ExprList, JoinType, LogicalJoin, LogicalProjection, LogicalScan, OptRelNode, OptRelNodeTyp
        },
        rules::ProjectionPushDownJoin,
        testing::new_test_optimizer,
    };

    #[test]
    fn proj_pushdown_join_adv_1() {
        // convert proj -> join -> 2xscan to join -> 2xproj -> 2xscan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectionPushDownJoin::new()));

        let left_scan = LogicalScan::new("region".into());
        let right_scan = LogicalScan::new("customer".into());

        let join_cond = BinOpExpr::new(
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let join_node = LogicalJoin::new(left_scan.into_plan_node(), right_scan.into_plan_node(), join_cond, JoinType::Inner);
        let proj = LogicalProjection::new(join_node.into_plan_node(), proj_exprs);

        let plan = test_optimizer.optimize(proj.into_rel_node()).unwrap();

        let left_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ])
        .into_rel_node();

        let right_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ])
        .into_rel_node();

        let new_join_cond = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
            BinOpType::Eq,
        )
        .into_expr().into_rel_node();

        assert_eq!(plan.typ, OptRelNodeTyp::Join(JoinType::Inner));
        assert_eq!(plan.child(2), new_join_cond);

        assert_eq!(plan.child(0).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(1), left_proj_exprs);
        assert_eq!(plan.child(1).child(1), right_proj_exprs);

        assert_eq!(plan.child(0).child(0).typ, OptRelNodeTyp::Scan);
        assert_eq!(plan.child(1).child(0).typ, OptRelNodeTyp::Scan);
    }

    #[test]
    fn proj_pushdown_join_adv_2() {
        // convert proj -> join -> 2xscan to proj -> join -> 2xproj -> 2xscan (out of order left + right cols)
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectionPushDownJoin::new()));

        let left_scan = LogicalScan::new("region".into());
        let right_scan = LogicalScan::new("customer".into());

        let join_cond = BinOpExpr::new(
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let join_node = LogicalJoin::new(left_scan.into_plan_node(), right_scan.into_plan_node(), join_cond, JoinType::Inner);
        let proj = LogicalProjection::new(join_node.into_plan_node(), proj_exprs);

        let plan = test_optimizer.optimize(proj.into_rel_node()).unwrap();

        let left_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ])
        .into_rel_node();

        let right_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ])
        .into_rel_node();

        let new_join_cond = BinOpExpr::new(
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
            BinOpType::Eq,
        )
        .into_expr().into_rel_node();

        let top_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
        ])
        .into_rel_node();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), top_proj_exprs);

        assert_eq!(plan.child(0).typ, OptRelNodeTyp::Join(JoinType::Inner));
        assert_eq!(plan.child(0).child(2), new_join_cond);

        assert_eq!(plan.child(0).child(0).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(1).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(0).child(1), left_proj_exprs);
        assert_eq!(plan.child(0).child(1).child(1), right_proj_exprs);

        assert_eq!(plan.child(0).child(0).child(0).typ, OptRelNodeTyp::Scan);
        assert_eq!(plan.child(0).child(1).child(0).typ, OptRelNodeTyp::Scan);
    }

    #[test]
    fn proj_pushdown_join_adv_3() {
        // convert proj -> join -> 2xscan to proj -> join -> 2xproj -> 2xscan (join cols not in proj cols)
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectionPushDownJoin::new()));

        let left_scan = LogicalScan::new("region".into());
        let right_scan = LogicalScan::new("customer".into());

        let join_cond = BinOpExpr::new(
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(7).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let join_node = LogicalJoin::new(left_scan.into_plan_node(), right_scan.into_plan_node(), join_cond, JoinType::Inner);
        let proj = LogicalProjection::new(join_node.into_plan_node(), proj_exprs);

        let plan = test_optimizer.optimize(proj.into_rel_node()).unwrap();

        let left_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ])
        .into_rel_node();

        let right_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
        ])
        .into_rel_node();

        let new_join_cond = BinOpExpr::new(
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            BinOpType::Eq,
        )
        .into_expr().into_rel_node();

        let top_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ])
        .into_rel_node();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), top_proj_exprs);

        assert_eq!(plan.child(0).typ, OptRelNodeTyp::Join(JoinType::Inner));
        assert_eq!(plan.child(0).child(2), new_join_cond);

        assert_eq!(plan.child(0).child(0).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(1).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(0).child(1), left_proj_exprs);
        assert_eq!(plan.child(0).child(1).child(1), right_proj_exprs);

        assert_eq!(plan.child(0).child(0).child(0).typ, OptRelNodeTyp::Scan);
        assert_eq!(plan.child(0).child(1).child(0).typ, OptRelNodeTyp::Scan);
    }

    #[test]
    fn proj_pushdown_join_adv_4() {
        // convert proj -> join -> 2xscan to proj -> join -> 2xproj -> 2xscan (partial join cols not in proj cols)
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectionPushDownJoin::new()));

        let left_scan = LogicalScan::new("region".into());
        let right_scan = LogicalScan::new("customer".into());

        let join_cond = BinOpExpr::new(
            ColumnRefExpr::new(3).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
            BinOpType::Eq,
        )
        .into_expr();

        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(4).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ]);

        let join_node = LogicalJoin::new(left_scan.into_plan_node(), right_scan.into_plan_node(), join_cond, JoinType::Inner);
        let proj = LogicalProjection::new(join_node.into_plan_node(), proj_exprs);

        let plan = test_optimizer.optimize(proj.into_rel_node()).unwrap();

        let left_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
        ])
        .into_rel_node();

        let right_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(0).into_expr(),
        ])
        .into_rel_node();

        let new_join_cond = BinOpExpr::new(
            ColumnRefExpr::new(3).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
            BinOpType::Eq,
        )
        .into_expr().into_rel_node();

        let top_proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
            ColumnRefExpr::new(3).into_expr(),
        ])
        .into_rel_node();

        assert_eq!(plan.typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(1), top_proj_exprs);

        assert_eq!(plan.child(0).typ, OptRelNodeTyp::Join(JoinType::Inner));
        assert_eq!(plan.child(0).child(2), new_join_cond);

        assert_eq!(plan.child(0).child(0).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(1).typ, OptRelNodeTyp::Projection);
        assert_eq!(plan.child(0).child(0).child(1), left_proj_exprs);
        assert_eq!(plan.child(0).child(1).child(1), right_proj_exprs);

        assert_eq!(plan.child(0).child(0).child(0).typ, OptRelNodeTyp::Scan);
        assert_eq!(plan.child(0).child(1).child(0).typ, OptRelNodeTyp::Scan);
    }
}
