//! Type definitions for relational (logical and physical) operators.

pub mod logical;
pub mod physical;

/// Trait for getting the children relations and children scalars of a relational operator.
pub trait RelationChildren {
    type Relation: Clone;
    type Scalar: Clone;

    /// Gets the children relations of this relational operator.
    fn children_relations(&self) -> Vec<Self::Relation>;

    /// Gets the children scalars of this relational operator.
    fn children_scalars(&self) -> Vec<Self::Scalar>;
}
