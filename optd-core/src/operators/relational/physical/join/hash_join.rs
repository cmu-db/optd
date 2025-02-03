use crate::operator::relational::{logical::join::JoinType, RelationChildren};

/// Hash-based join operator that matches rows based on equality conditions.
///
/// Takes left and right input relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Builds hash table from build side (right)
/// and probes with rows from probe side (left).
#[derive(Clone)]
<<<<<<< HEAD:optd-core/src/operators/relational/physical/join/hash_join.rs
pub struct HashJoin<Metadata, Relation, Scalar> {
    pub join_type: Metadata,
=======
pub struct HashJoin<Relation, Scalar> {
    pub join_type: JoinType,
>>>>>>> origin/yuchen/initial-storage:optd-core/src/operator/relational/physical/join/hash_join.rs
    /// Left relation that probes hash table.
    pub probe_side: Relation,
    /// Right relation used to build hash table.
    pub build_side: Relation,
    pub condition: Scalar,
}

impl<Relation, Scalar> RelationChildren for HashJoin<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        vec![self.probe_side.clone(), self.build_side.clone()]
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        vec![self.condition.clone()]
    }
}
