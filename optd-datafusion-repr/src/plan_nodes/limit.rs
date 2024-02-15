use super::{macros::define_plan_node, Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalLimit(pub PlanNode);

define_plan_node!(
    LogicalLimit : PlanNode,
    Limit, [
        { 0, child: PlanNode }
    ], [
        { 1, skip: Expr },
        { 2, fetch: Expr }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalLimit(pub PlanNode);

define_plan_node!(
    PhysicalLimit : PlanNode,
    PhysicalLimit, [
        { 0, child: PlanNode }
    ], [
        { 1, skip: Expr },
        { 2, fetch: Expr }
    ]
);
