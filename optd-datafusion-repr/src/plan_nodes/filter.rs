use crate::define_plan_node;

use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalFilter(pub PlanNode);

define_plan_node!(
    LogicalFilter : PlanNode,
    Filter, [
        { 0, child: PlanNode }
    ], [
        { 1, cond: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalFilter(pub PlanNode);

define_plan_node!(
    PhysicalFilter : PlanNode,
    PhysicalFilter, [
        { 0, child: PlanNode }
    ], [
        { 1, cond: Expr }
    ]
);
