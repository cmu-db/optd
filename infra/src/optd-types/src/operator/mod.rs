use std::{marker::PhantomData, sync::Arc};

pub mod logical;
pub mod physical;

/// Note: This type is generic over `Link` which specifies what kind of children this operator is
/// allowed to have.
///
/// TODO figure out fields.
pub struct ScalarOperator<Link> {
    _phantom: PhantomData<Link>,
}
