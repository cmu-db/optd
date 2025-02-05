//! A scalar addition operator.

use crate::{operators::scalar::ScalarOperator, values::OptdValue};
use serde::Deserialize;

/// A scalar operator that adds two values.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Add<Scalar> {
    /// The left operand.
    pub left: Scalar,
    /// The right operand.
    pub right: Scalar,
}

impl<Scalar> Add<Scalar> {
    /// Create a new addition operator.
    pub fn new(left: Scalar, right: Scalar) -> Self {
        Self { left, right }
    }
}

/// Creates an addition scalar operator.
pub fn add<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::Add(Add::new(left, right))
}
