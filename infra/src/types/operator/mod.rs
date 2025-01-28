use std::{marker::PhantomData, sync::Arc};

use super::plan::logical_plan::ScalarLink;

pub mod logical;
pub mod physical;

/// Note: This type is generic over `Link` which specifies what kind of children this operator is
/// allowed to have.
///
/// TODO figure out fields.
pub struct ScalarOperator<Link> {
    _phantom: PhantomData<Link>,
}

impl ScalarOperator<ScalarLink> {
    // Add a public constructor
    pub fn new() -> Self {
        ScalarOperator {
            _phantom: std::marker::PhantomData,
        }
    }
}
