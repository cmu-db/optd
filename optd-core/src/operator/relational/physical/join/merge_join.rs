/// Merge join operator that matches rows based on equality conditions.
///
/// Takes sorted left and right relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Both inputs must be sorted on join keys.
#[derive(Clone)]
pub struct MergeJoin<Relation, Scalar> {
    pub join_type: String,
    pub left_sorted: Relation,  // Left sorted relation.
    pub right_sorted: Relation, // Right sorted relation.
    pub condition: Scalar,
}
