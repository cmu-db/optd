use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PhysicalExprList, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalSort(pub PlanNode);

// each expression in ExprList is represented as a SortOrderExpr
// 1. nulls_first is not included from DF
// 2. node type defines sort order per expression
// 3. actual expr is stored as a child of this node
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
        { 1, exprs: PhysicalExprList }
    ]
);
