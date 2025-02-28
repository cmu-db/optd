//! A scalar constant operator.

use crate::{cascades::ir::OperatorData, operators::scalar::ScalarOperator};
use serde::{Deserialize, Serialize};

/// A scalar operator representing a constant value.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Constant<Value> {
    /// The constant value.
    pub value: Value,
}

impl Constant<OperatorData> {
    /// Create a new constant.
    pub fn new(value: OperatorData) -> Self {
        Self { value }
    }
}

/// Creates a boolean constant scalar operator.
pub fn boolean<Scalar>(value: bool) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::Constant(Constant::new(OperatorData::Bool(value)))
}

/// Creates an `int64` constant scalar operator.
pub fn int64<Scalar>(value: bool) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::Constant(Constant::new(OperatorData::Bool(value)))
}

/// Creates a string constant scalar operator.
pub fn string<Scalar>(value: &str) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::Constant(Constant::new(OperatorData::String(value.into())))
}
