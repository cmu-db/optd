//! A scalar logic operator.

use serde::Deserialize;

use crate::cascades::ir::OperatorData;

use super::ScalarOperator;

/// A scalar operator that adds two values.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LogicOp<Value, Scalar> {
    /// The kind of logic operator.
    pub kind: Value,
    /// The operands to the logic operator.
    pub children: Vec<Scalar>,
}

impl<Value, Scalar> LogicOp<Value, Scalar> {
    /// Create a new logic scalar operator.
    pub fn new(kind: Value, children: Vec<Scalar>) -> Self {
        Self { kind, children }
    }
}

/// Creates an `and` logic scalar operator.
pub fn and<Scalar>(children: Vec<Scalar>) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::LogicOp(LogicOp::new(OperatorData::String("and".into()), children))
}

/// Creates an `and` logic scalar operator.
pub fn or<Scalar>(children: Vec<Scalar>) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::LogicOp(LogicOp::new(OperatorData::String("or".into()), children))
}
