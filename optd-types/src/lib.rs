#![allow(unused)]

mod expression;
mod memo;
mod operator;
mod plan;

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
pub struct LogicalExprId(u64);

/// TODO Add docs.
pub struct PhysicalExprId(u64);
