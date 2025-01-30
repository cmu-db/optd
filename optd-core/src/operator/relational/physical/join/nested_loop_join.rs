use crate::operator::relational::RelationChildren;

/// Nested-loop join operator that matches rows based on a predicate.
///
/// Takes outer and inner relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Scans inner relation for each outer row.
#[derive(Clone)]
pub struct NestedLoopJoin<Relation, Scalar> {
    pub join_type: String,
    /// Outer relation.
    pub outer: Relation,
    /// Inner relation scanned for each outer row.
    pub inner: Relation,
    pub condition: Scalar,
}

impl<Relation, Scalar> RelationChildren for NestedLoopJoin<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        vec![self.outer.clone(), self.inner.clone()]
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        vec![self.condition.clone()]
    }
}
