use optd_core::rel_node::Value;

use super::{
    macros::define_plan_node, Expr, ExprList, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode,
};

#[derive(Clone, Debug)]
pub struct LogicalScan(pub PlanNode);

define_plan_node!(
    LogicalScan : PlanNode,
    Scan, [], [
        { 0, filter: Expr },
        { 1, projections: ExprList },
        { 2, fetch: Expr }
    ], table_name
);

#[derive(Clone, Debug)]
pub struct PhysicalScan(pub PlanNode);

define_plan_node!(
    PhysicalScan : PlanNode,
    PhysicalScan, [], [
        { 0, filter: Expr },
        { 1, projections: ExprList },
        { 2, fetch: Expr }
    ], table_name
);
