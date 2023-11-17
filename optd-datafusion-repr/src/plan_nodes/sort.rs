use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalSort(pub PlanNode);

define_plan_node!(
    LogicalSort : PlanNode,
    Sort, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalSort(pub PlanNode);

define_plan_node!(
    PhysicalSort : PlanNode,
    PhysicalSort, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);
