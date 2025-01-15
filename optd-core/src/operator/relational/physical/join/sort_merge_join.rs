/// Sort-merge join operator that matches rows based on equality conditions.
///
/// Takes sorted left and right relations (`RelLink`) and joins their rows using
/// a join condition (`ScalarLink`). Both inputs must be sorted on join keys.
#[derive(Clone)]
pub struct SortMergeJoin<RelLink, ScalarLink> {
    pub join_type: String,
    pub left_sorted: RelLink,  // Left sorted relation.
    pub right_sorted: RelLink, // Right sorted relation.
    pub condition: ScalarLink,
}
