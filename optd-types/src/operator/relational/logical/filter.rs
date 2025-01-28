/// Logical filter operator that selects rows matching a condition
///
/// Takes input relation (`RelLink`) and filters rows using a boolean 
/// predicate (`ScalarLink`).
pub struct FilterOperator<RelLink, ScalarLink> {
    pub child: RelLink,
    pub predicate: ScalarLink,
 }