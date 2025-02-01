//! A logical filter.

use crate::operator::relational::RelationChildren;

/// Logical filter operator that selects rows matching a condition.
///
/// Takes input relation (`Relation`) and filters rows using a boolean predicate (`Scalar`).
#[derive(Clone)]
pub struct Filter<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// The filter expression denoting the predicate condition for this filter operation.
    ///
    /// For example, a filter predicate could be `column_a > 42`, or it could be something like
    /// `column_b < 100 AND column_c > 1000`.
    pub predicate: Scalar,
}

impl<Relation, Scalar> RelationChildren for Filter<Relation, Scalar>
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
        vec![self.predicate.clone()]
    }
}
