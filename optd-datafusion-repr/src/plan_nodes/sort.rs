use super::macros::define_plan_node;
use super::{ArcDfPlanNode, DfNodeType, DfPlanNode, DfReprPlanNode, ListPred};

#[derive(Clone, Debug)]
pub struct LogicalSort(pub ArcDfPlanNode);

// each expression in ExprList is represented as a SortOrderExpr
// 1. nulls_first is not included from DF
// 2. node type defines sort order per expression
// 3. actual expr is stored as a child of this node
define_plan_node!(
    LogicalSort : DfPlanNode,
    Sort, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, exprs: ListPred }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalSort(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalSort : DfPlanNode,
    PhysicalSort, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, exprs: ListPred }
    ]
);
