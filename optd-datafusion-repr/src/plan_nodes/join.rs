use core::fmt;
use std::fmt::Display;

use super::macros::define_plan_node;
use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PhysicalExprList, PlanNode};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
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

#[derive(Clone, Debug)]
pub struct PhysicalHashJoin(pub PlanNode);

define_plan_node!(
    PhysicalHashJoin : PlanNode,
    PhysicalHashJoin, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, left_keys: Expr },
        { 3, right_keys: Expr }
    ], { join_type: JoinType }
);

impl LogicalJoin {
    /// Takes in left/right schema sizes, and maps a column index to be as if it
    /// were pushed down to the left or right side of a join accordingly.
    pub fn map_through_join(
        col_idx: usize,
        left_schema_size: usize,
        right_schema_size: usize,
    ) -> usize {
        assert!(col_idx < left_schema_size + right_schema_size);
        if col_idx < left_schema_size {
            col_idx
        } else {
            col_idx - left_schema_size
        }
    }
}
