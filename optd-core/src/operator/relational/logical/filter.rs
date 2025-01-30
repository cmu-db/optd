use crate::operator::relational::RelationChildren;

/// Logical filter operator that selects rows matching a condition.
///
/// Takes input relation (`Relation`) and filters rows using a boolean predicate (`Scalar`).
#[derive(Clone)]
pub struct Filter<Relation, Scalar> {
    pub child: Relation,
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
