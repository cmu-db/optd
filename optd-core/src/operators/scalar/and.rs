//! A scalar addition operator.

use serde::Deserialize;

use crate::values::OptdValue;

use super::ScalarOperator;

/// A scalar operator that adds two values.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct And<Scalar> {
    /// The left operand.
    pub left: Scalar,
    /// The right operand.
    pub right: Scalar,
}

impl<Scalar> And<Scalar> {
    /// Create a new `And` operator.
    pub fn new(left: Scalar, right: Scalar) -> Self {
        Self { left, right }
    }
}

/// Creates an `And` scalar operator.
pub fn and<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::And(And::new(left, right))
}
