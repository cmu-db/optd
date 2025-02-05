//! A logical join.

use super::LogicalOperator;
use crate::values::OptdValue;
use serde::Deserialize;

/// Logical join operator that combines rows from two relations.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Join<Value, Relation, Scalar> {
    /// The type of join (inner, left, right).
    pub join_type: Value,
    /// The left input relation.
    pub left: Relation,
    /// The right input relation.
    pub right: Relation,
    /// The join condition.
    pub condition: Scalar,
}

impl<Relation, Scalar> Join<OptdValue, Relation, Scalar> {
    /// Create a new join operator.
    pub fn new(join_type: &str, left: Relation, right: Relation, condition: Scalar) -> Self {
        Self {
            join_type: OptdValue::String(join_type.into()),
            left,
            right,
            condition,
        }
    }
}

/// Creates a join logical operator.
pub fn join<Relation, Scalar>(
    join_type: &str,
    left: Relation,
    right: Relation,
    condition: Scalar,
) -> LogicalOperator<OptdValue, Relation, Scalar> {
    LogicalOperator::Join(Join::new(join_type, left, right, condition))
}
