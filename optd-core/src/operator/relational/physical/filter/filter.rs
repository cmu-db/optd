use crate::operator::relational::RelationChildren;

/// Physical filter operator that applies a boolean predicate to filter input rows.
///
/// Takes a child operator (`Relation`) providing input rows and a predicate expression
/// (`Scalar`) that evaluates to true/false. Only rows where predicate is true
/// are emitted.
#[derive(Clone)]
pub struct PhysicalFilter<Relation, Scalar> {
    pub child: Relation,
    pub predicate: Scalar,
}

impl<Relation, Scalar> RelationChildren for PhysicalFilter<Relation, Scalar>
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
