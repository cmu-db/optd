//! All types supported by the DSL, including the base types and the complex, user-defined types.

use std::sync::Arc;

#[derive(Clone)]
pub enum UserType {
    /// The base types supported by the OPTD-DSL.
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Bool(bool),
    Unit(()),
    /// The complex, user-defined types supported by the OPTD-DSL.
    Enum(Vec<Arc<UserType>>),
    Array(Vec<Arc<UserType>>),
}
