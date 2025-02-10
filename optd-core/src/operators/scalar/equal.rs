//! A scalar equality operator.

use crate::{operators::scalar::ScalarOperator, values::OptdValue};
use serde::Deserialize;

/// A scalar operator that compares two values for equality.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Equal<Scalar> {
    /// The left operand.
    pub left: Scalar,
    /// The right operand.
    pub right: Scalar,
}

impl<Scalar> Equal<Scalar> {
    /// Create a new equality operator.
    pub fn new(left: Scalar, right: Scalar) -> Self {
        Self { left, right }
    }
}

/// Creates an equality scalar operator.
pub fn equal<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::Equal(Equal::new(left, right))
}
