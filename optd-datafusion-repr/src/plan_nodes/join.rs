use core::fmt;
use std::fmt::Display;

use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};
use crate::define_plan_node;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
    Cross,
    Semi,
    AntiSemi,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalJoin(pub PlanNode);

define_plan_node!(
    LogicalJoin : PlanNode,
    Join, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct PhysicalNestedLoopJoin(pub PlanNode);

define_plan_node!(
    PhysicalNestedLoopJoin : PlanNode,
    PhysicalNestedLoopJoin, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr }
    ], { join_type: JoinType }
);
