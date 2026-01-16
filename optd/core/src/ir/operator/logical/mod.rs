//! Logical operators are the high-level representation of query plans before
//! a physical execution strategy is chosen. They focus on the logical
//! operations to be performed, such as filtering, projecting, joining, and
//! aggregating data.

pub mod aggregate;
pub mod get;
pub mod join;
pub mod order_by;
pub mod project;
pub mod remap;
pub mod select;
