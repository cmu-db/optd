/// Hash-based join operator that matches rows based on equality conditions.
///
/// Takes left and right input relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Builds hash table from build side (right)
/// and probes with rows from probe side (left).
#[derive(Clone)]
pub struct HashJoin<Relation, Scalar> {
    pub join_type: String,
    pub probe_side: Relation, // Left relation that probes hash table.
    pub build_side: Relation, // Right relation used to build hash table.
    pub condition: Scalar,
}
