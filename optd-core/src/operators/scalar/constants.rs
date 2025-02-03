use serde::{Deserialize, Serialize};

use super::ScalarOperator;

/// Constants that can appear in scalar expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constant<Metadata> {
    value: Metadata,
}

// TODO(alexis): problemmmmm
pub fn boolean<Scalar>(value: bool) -> ScalarOperator<Scalar> {
    ScalarOperator::Constant(Constant::Boolean(value))
}

pub fn integer<Scalar>(value: i64) -> ScalarOperator<Scalar> {
    ScalarOperator::Constant(Constant::Integer(value))
}
