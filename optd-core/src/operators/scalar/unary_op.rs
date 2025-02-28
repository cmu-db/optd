//! A scalar binary operator.
use crate::{cascades::ir::OperatorData, operators::scalar::ScalarOperator};
use serde::Deserialize;

/// A scalar operator that performs a unary operation on its child.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UnaryOp<Value, Scalar> {
    /// The kind of operator.
    pub kind: Value,
    /// The child operand.
    pub child: Scalar,
}

impl<Value, Scalar> UnaryOp<Value, Scalar> {
    /// Create a new addition operator.
    pub fn new(kind: Value, child: Scalar) -> Self {
        Self { kind, child }
    }
}

/// Creates a not unary scalar operator (e.g. `NOT true`).
pub fn not<Scalar>(child: Scalar) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::UnaryOp(UnaryOp::new(OperatorData::String("not".into()), child))
}

/// Creates a negation unary scalar operator (e.g. `-1`).
pub fn neg<Scalar>(child: Scalar) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::UnaryOp(UnaryOp::new(OperatorData::String("neg".into()), child))
}
