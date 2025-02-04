use serde::Deserialize;

/// Equality operator takes in two scalar values of the same type
/// and checks if they are equal.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Equal<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}

impl<Scalar> Equal<Scalar> {
    pub fn new(left: Scalar, right: Scalar) -> Self {
        Equal { left, right }
    }
}
