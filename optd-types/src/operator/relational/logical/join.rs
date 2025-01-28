/// Logical join operator that combines rows from two relations
///
/// Takes left and right relations (`RelLink`) and joins their rows using
/// a join condition (`ScalarLink`).
pub struct JoinOperator<RelLink, ScalarLink> {
    pub join_type: String,
    pub left: RelLink,
    pub right: RelLink, 
    pub condition: ScalarLink,
 }