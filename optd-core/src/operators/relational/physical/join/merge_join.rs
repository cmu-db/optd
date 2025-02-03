use crate::operator::relational::{logical::join::JoinType, RelationChildren};

/// Merge join operator that matches rows based on equality conditions.
///
/// Takes sorted left and right relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Both inputs must be sorted on join keys.
#[derive(Clone)]
<<<<<<< HEAD:optd-core/src/operators/relational/physical/join/merge_join.rs
pub struct MergeJoin<Metadata, Relation, Scalar> {
    pub join_type: Metadata,
=======
pub struct MergeJoin<Relation, Scalar> {
    pub join_type: JoinType,
>>>>>>> origin/yuchen/initial-storage:optd-core/src/operator/relational/physical/join/merge_join.rs
    /// Left sorted relation.
    pub left_sorted: Relation,
    /// Right sorted relation.
    pub right_sorted: Relation,
    pub condition: Scalar,
}

impl<Relation, Scalar> RelationChildren for MergeJoin<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        vec![self.left_sorted.clone(), self.right_sorted.clone()]
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        vec![self.condition.clone()]
    }
}
