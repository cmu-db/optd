//! A scalar binary operator.
use crate::{cascades::ir::OperatorData, operators::scalar::ScalarOperator};
use serde::Deserialize;

/// A scalar operator that performs a binary operation on two values.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BinaryOp<Value, Scalar> {
    /// The kind of operator.
    pub kind: Value,
    /// The left operand.
    pub left: Scalar,
    /// The right operand.
    pub right: Scalar,
}

impl<Value, Scalar> BinaryOp<Value, Scalar> {
    /// Create a new addition operator.
    pub fn new(kind: Value, left: Scalar, right: Scalar) -> Self {
        Self { kind, left, right }
    }
}

/// Creates an addition scalar operator.
pub fn add<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::BinaryOp(BinaryOp::new(
        OperatorData::String("add".into()),
        left,
        right,
    ))
}

pub fn minus<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::BinaryOp(BinaryOp::new(
        OperatorData::String("minus".into()),
        left,
        right,
    ))
}

/// Creates an equality scalar operator.
pub fn equal<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::BinaryOp(BinaryOp::new(
        OperatorData::String("equal".into()),
        left,
        right,
    ))
}
