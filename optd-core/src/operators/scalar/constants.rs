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

/// Creates a boolean constant scalar operator.
pub fn boolean<Scalar>(value: bool) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::Constant(Constant::new(OptdValue::Bool(value)))
}

/// Creates an `int64` constant scalar operator.
pub fn int64<Scalar>(value: bool) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::Constant(Constant::new(OptdValue::Bool(value)))
}

/// Creates a string constant scalar operator.
pub fn string<Scalar>(value: &str) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::Constant(Constant::new(OptdValue::String(value.into())))
}
