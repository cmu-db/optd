//! A logical join.

use serde::Deserialize;

use crate::values::OptdValue;

/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Join<Value, Relation, Scalar> {
    pub join_type: Value,
    pub left: Relation,
    /// The right input relation.
    pub right: Relation,
    /// The join expression denoting the join condition that links the two input relations.
    ///
    /// For example, a join operation could have a condition on `t1.id = t2.id` (an equijoin).
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
