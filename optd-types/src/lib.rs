#![allow(unused)]

mod expression;
mod memo;
mod plan;

/// TODO make distinction between relational groups and scalar groups.
pub struct GroupId(u64);

pub struct LogicalExprId(u64);

pub struct PhysicalExprId(u64);
