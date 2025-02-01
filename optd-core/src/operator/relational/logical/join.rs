//! A logical join.

use crate::operator::relational::RelationChildren;

/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Clone)]
pub struct Join<Relation, Scalar> {
    /// TODO(alexis) Mocked for now.
    pub join_type: String,
    /// The left input relation.
    pub left: Relation,
    /// The right input relation.
    pub right: Relation,
    /// The join expression denoting the join condition that links the two input relations.
    ///
    /// For example, a join operation could have a condition on `t1.id = t2.id` (an equijoin).
    pub condition: Scalar,
}

impl<Relation, Scalar> RelationChildren for Join<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        vec![self.condition.clone()]
    }
}
