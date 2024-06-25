// intended to remove a projection that outputs the same num of cols 
// that are in scan node
use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{ColumnRefExpr, ExprList, OptRelNode, OptRelNodeTyp, PlanNode};
use crate::properties::schema::SchemaPropertyBuilder;
use crate::rules::macros::define_rule;

// Proj (Scan A) -> Scan A
// removes projections
// TODO: need to somehow match on just scan node instead
// only works in hueristic optimizer (which may be ok)
// ideally include a pass after for physical proj -> physical scan
define_rule!(
    ProjectRemoveRule,
    apply_projection_remove,
    (Projection, child, [exprs])
);

fn apply_projection_remove(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectRemoveRulePicks {
        child,
        exprs
    }: ProjectRemoveRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let child_schema = optimizer.get_property::<SchemaPropertyBuilder>(child.clone().into(), 0);
    let child = PlanNode::from_group(child.into());
    if child.typ() != OptRelNodeTyp::Scan {
        return vec![];
    }
    let exprs = ExprList::from_rel_node(exprs.into()).unwrap().to_vec();
    if exprs.len() != child_schema.len() {
        return vec![];
    }
    let mut exp_col_idx: usize = 0;
    for expr in exprs {
        let col_ref = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
        let col_idx = col_ref.index();
        if exp_col_idx != col_idx {
            return vec![];
        }
        exp_col_idx += 1;
    }
    vec![child.into_rel_node().as_ref().clone()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimizer::Optimizer;

    use crate::{
        plan_nodes::{
            ColumnRefExpr, ExprList, LogicalProjection, LogicalScan, OptRelNode, OptRelNodeTyp,
        },
        rules::ProjectRemoveRule,
        testing::new_test_optimizer,
    };

    #[test]
    fn proj_scan_basic() {
        // convert proj -> scan to scan
        let mut test_optimizer = new_test_optimizer(Arc::new(ProjectRemoveRule::new()));

        let scan = LogicalScan::new("region".into());

        let proj_exprs = ExprList::new(vec![
            ColumnRefExpr::new(0).into_expr(),
            ColumnRefExpr::new(1).into_expr(),
            ColumnRefExpr::new(2).into_expr(),
        ]);

        let proj_node: LogicalProjection = LogicalProjection::new(scan.into_plan_node(), proj_exprs);
        let plan = test_optimizer.optimize(proj_node.into_rel_node()).unwrap();

        assert_eq!(plan.typ, OptRelNodeTyp::Scan);
    }
}
