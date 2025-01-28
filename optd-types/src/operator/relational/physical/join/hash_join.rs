/// Hash-based join operator that matches rows based on equality conditions
///
/// Takes left and right input relations (`RelLink`) and joins their rows using
/// a join condition (`ScalarLink`). Builds hash table from build side (right)
/// and probes with rows from probe side (left).
pub struct HashJoinOperator<RelLink, ScalarLink> {
    pub join_type: String,
    pub probe_side: RelLink, // Left relation that probes hash table
    pub build_side: RelLink, // Right relation used to build hash table
    pub condition: ScalarLink,
}
