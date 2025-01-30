use crate::operator::relational::RelationChildren;

/// Column projection operator that transforms input rows.
///
/// Takes input relation (`Relation`) and projects columns/expressions (`Scalar`)
/// to produce output rows with selected/computed fields.
#[derive(Clone)]
pub struct Project<Relation, Scalar> {
    pub child: Relation,
    pub fields: Vec<Scalar>,
}

impl<Relation, Scalar> RelationChildren for Project<Relation, Scalar>
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
        self.fields.clone()
    }
}
