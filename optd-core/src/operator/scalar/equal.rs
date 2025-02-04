/// Binary And expression for scalar values.
#[derive(Clone)]
pub struct Equal<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}
