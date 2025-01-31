/// Binary And expression for scalar values.
#[derive(Clone)]
pub struct And<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}
