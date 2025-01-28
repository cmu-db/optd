//! This module contains items related to query plan operators, both relational (logical / physical)
//! and scalar.

pub mod logical;
pub mod physical;

/// Note: This type is generic over `Link` which specifies what kind of children this operator is
/// allowed to have.
///
/// TODO figure out fields.
#[derive(Clone)]
pub struct Scalar {}
