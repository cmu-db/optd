//! A logical projection.

use serde::Deserialize;

use crate::operator::relational::RelationChildren;

/// Logical project operator that specifies output columns.
///
/// Takes input relation (`Relation`) and defines output columns/expressions
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Project<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// TODO(everyone): What exactly is going on here?
    pub fields: Vec<Scalar>,
}

impl<Relation, Scalar> Project<Relation, Scalar> {
    /// Create a new project operator.
    pub fn new(child: Relation, fields: Vec<Scalar>) -> Self {
        Self { child, fields }
    }
}

impl<Relation, Scalar> RelationChildren for Project<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        vec![self.child.clone()]
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        self.fields.clone()
    }
}
