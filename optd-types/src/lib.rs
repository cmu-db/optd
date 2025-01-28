#![allow(unused_variables)]

pub mod expression;
pub mod memo;
pub mod operator;
pub mod plan;
pub mod rules;

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
#[allow(dead_code)]
pub struct ExprId(u64);
