use serde::{Deserialize, Serialize};

use super::ScalarOperator;

/// Constants that can appear in scalar expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Constant {
    /// String constant (e.g. "hello").
    String(String),
    /// Integer constant (e.g. 42).
    Integer(i64),
    /// Floating point constant (e.g. 3.14).
    Float(f64),
    /// Boolean constant (e.g. true, false).
    Boolean(bool),
}

pub fn boolean<Scalar>(value: bool) -> ScalarOperator<Scalar> {
    ScalarOperator::Constant(Constant::Boolean(value))
}

pub fn integer<Scalar>(value: i64) -> ScalarOperator<Scalar> {
    ScalarOperator::Constant(Constant::Integer(value))
}
