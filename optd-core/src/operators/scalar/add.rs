use serde::Deserialize;

/// The addition scalar operator takes in two scalar values
/// of the same type and produces their sum.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Add<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}

impl<Scalar> Add<Scalar> {
    /// Create a new addition operator.
    pub fn new(left: Scalar, right: Scalar) -> Self {
        Self { left, right }
    }
}
