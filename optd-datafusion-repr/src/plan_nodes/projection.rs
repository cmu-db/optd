use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub PlanNode);

define_plan_node!(
    LogicalProjection : PlanNode,
    Projection, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub PlanNode);

define_plan_node!(
    PhysicalProjection : PlanNode,
    PhysicalProjection, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);
