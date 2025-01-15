/// Logical filter operator that selects rows matching a condition.
///
/// Takes input relation (`RelLink`) and filters rows using a boolean predicate (`ScalarLink`).
#[derive(Clone)]
pub struct Filter<RelLink, ScalarLink> {
    pub child: RelLink,
    pub predicate: ScalarLink,
}
