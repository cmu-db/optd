use serde::Deserialize;

use super::ScalarOperator;

/// Equality operator takes in two scalar values of the same type
/// and checks if they are equal.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Equal<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}

/// Create a new equality operator.
/// TODO(alexis): Same problem...
pub fn equal<Scalar>(left: Scalar, right: Scalar) -> ScalarOperator<Scalar> {
    ScalarOperator::Equal(Equal { left, right })
}
