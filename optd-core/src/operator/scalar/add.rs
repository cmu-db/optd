/// Addition expression for scalar values.
#[derive(Clone)]
pub struct Add<Scalar> {
    pub left: Scalar,
    pub right: Scalar,
}
