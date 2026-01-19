//! Physical operators are the implementation-level representation of query
//! plans before after an execution strategy is chosen. They focus on the
//! actual operations to be performed, such as hash-joining vs loop-joining, etc

pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod mock_scan;
pub mod nl_join;
pub mod project;
pub mod table_scan;
