//! See comment in `optd-core/src/cascades/plan.rs` for more information about this extra IR.

use std::collections::HashSet;

use super::user_type::UserType;

/// The generic logical operator type of the internal, type-erased IR.
#[derive(Clone)]
pub struct LogicalOperator<Logical, Scalar> {
    pub op_type: String,
    pub content: Vec<HashSet<String, UserType>>,
    pub logical_children: Vec<Logical>,
    pub scalar_children: Vec<Scalar>,
}

/// The generic scalar operator type of the internal, type-erased IR.
#[derive(Clone)]
pub struct ScalarOperator<Scalar> {
    pub op_type: String,
    pub content: HashSet<String, UserType>,
    pub scalar_children: Vec<Scalar>,
}
