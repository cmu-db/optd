use std::marker::PhantomData;

use crate::operator::relational::RelationChildren;

/// Logical scan operator that reads from a base table.
///
/// Reads from table (`String`) and optionally filters rows using a pushdown predicate
/// (`Scalar`).
#[derive(Clone)]
pub struct Scan<Relation, Scalar> {
    pub table_name: String, // TODO(alexis): Mocked for now.
    pub predicate: Option<Scalar>,
    _phantom: PhantomData<Relation>,
}

impl<Relation, Scalar> RelationChildren for Scan<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        vec![]
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        self.predicate.iter().cloned().collect()
    }
}
