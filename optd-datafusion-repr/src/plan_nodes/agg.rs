use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalAgg(pub PlanNode);

define_plan_node!(
    LogicalAgg : PlanNode,
    Agg, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList },
        { 2, groups: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalAgg(pub PlanNode);

define_plan_node!(
    PhysicalAgg : PlanNode,
    PhysicalAgg, [
        { 0, child: PlanNode }
    ], [
        { 1, aggrs: ExprList },
        { 2, groups: ExprList }
    ]
);
