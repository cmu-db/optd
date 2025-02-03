//! A logical join.

use serde::Deserialize;

use crate::operator::relational::RelationChildren;

/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Join<Relation, Scalar> {
    /// The type of join.
    pub join_type: JoinType,
    /// The left input relation.
    pub left: Relation,
    /// The right input relation.
    pub right: Relation,
    /// The join expression denoting the join condition that links the two input relations.
    ///
    /// For example, a join operation could have a condition on `t1.id = t2.id` (an equijoin).
    pub condition: Scalar,
}

// TODO(yuchen): add support for other join types.
/// The type of join.
#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type, Deserialize)]
pub enum JoinType {
    /// Inner join.
    Inner,
}

impl<Relation, Scalar> Join<Relation, Scalar> {
    /// Create a new join operator.
    pub fn new(join_type: JoinType, left: Relation, right: Relation, condition: Scalar) -> Self {
        Self {
            join_type,
            left,
            right,
            condition,
        }
    }
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
