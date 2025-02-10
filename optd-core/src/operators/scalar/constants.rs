//! A scalar constant operator.

use crate::{operators::scalar::ScalarOperator, values::OptdValue};
use serde::{Deserialize, Serialize};

/// A scalar operator representing a constant value.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Constant<Value> {
    /// The constant value.
    pub value: Value,
}

impl Constant<OptdValue> {
    /// Create a new constant.
    pub fn new(value: OptdValue) -> Self {
        Self { value }
    }
}

/// Creates a constant scalar operator.
pub fn constant<Scalar>(value: OptdValue) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::Constant(Constant::new(value))
}
