use std::sync::Arc;

pub mod logical;
pub mod physical;

/// TODO potentially use the `smallvec` crate here instead.
pub struct Children<T>(Vec<Arc<T>>);

/// Note: This type is generic over `Link` which specifies what kind of children this operator is
/// allowed to have.
///
/// TODO figure out fields.
pub struct ScalarOperator<Link> {
    stuff: (),
    children: Children<Link>,
}
