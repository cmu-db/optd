/// Addition expression for scalar values.
#[derive(Clone)]
pub struct Add<ScalarLink> {
    pub left: ScalarLink,
    pub right: ScalarLink,
}
