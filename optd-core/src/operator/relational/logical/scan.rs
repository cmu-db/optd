//! A logical scan.

use std::marker::PhantomData;

use crate::operator::relational::RelationChildren;

/// Logical scan operator that reads from a base table.
///
/// Reads from table (`String`) and optionally filters rows using a pushdown predicate
/// (`Scalar`).
#[derive(Clone)]
pub struct Scan<Relation, Scalar> {
    /// TODO(alexis) Mocked for now.
    pub table_name: String,
    /// An optional filter expression for predicate pushdown into scan operators.
    ///
    /// For example, a `Filter(Scan(A), column_a < 42)` can be converted into a predicate pushdown
    /// `Scan(A, column < 42)` to prevent having to materialize many tuples.
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
