use std::marker::PhantomData;

use crate::operator::relational::RelationChildren;

/// Table scan operator that reads rows from a base table
///
/// Reads from table (`String`) and optionally filters rows using
/// a pushdown predicate (`Scalar`).
#[derive(Clone)]
pub struct TableScan<Relation, Scalar> {
    pub table_name: String, // TODO(alexis): Mocked for now.
    pub predicate: Scalar,
    _phantom: PhantomData<Relation>,
}

impl<Relation, Scalar> TableScan<Relation, Scalar> {
    /// Create a new table scan operator
    pub fn new(table_name: &str, predicate: Scalar) -> Self {
        Self {
            table_name: table_name.to_string(),
            predicate,
            _phantom: PhantomData,
        }
    }
}

impl<Relation, Scalar> RelationChildren for TableScan<Relation, Scalar>
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
