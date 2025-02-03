//! A logical scan.

use std::{fs::Metadata, marker::PhantomData};

use serde::Deserialize;

use crate::operator::relational::RelationChildren;

/// Logical scan operator that reads from a base table.
///
/// Reads from table (`String`) and optionally filters rows using a pushdown predicate
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Scan<Metadata, Scalar> {
    /// TODO(alexis) Mocked for now.
    pub table_name: Metadata,
    /// An optional filter expression for predicate pushdown into scan operators.
    ///
    /// For example, a `Filter(Scan(A), column_a < 42)` can be converted into a predicate pushdown
    /// `Scan(A, column < 42)` to prevent having to materialize many tuples.
    pub predicate: Scalar,
}

impl<Metadata, Scalar> Scan<Metadata, Scalar> {
    /// Create a new scan operator
    pub fn new(table_name: &str, predicate: Scalar) -> Self {
        Self {
            table_name: table_name.to_string(),
            predicate,
        }
    }
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
        vec![self.predicate.clone()]
    }
}
