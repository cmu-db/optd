use crate::operator::relational::RelationChildren;

/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Clone)]
pub struct Join<Relation, Scalar> {
    pub join_type: String,
    pub left: Relation,
    pub right: Relation,
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
