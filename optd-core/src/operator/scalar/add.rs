use serde::Deserialize;

use super::ScalarOperator;

/// The addition scalar operator takes in two scalar values
/// of the same type and produces their sum.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Add<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}

/// Create a new addition operator.
pub fn add<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<Scalar> {
    ScalarOperator::Add(Add { left, right })
}
