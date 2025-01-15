/// Nested-loop join operator that matches rows based on a predicate.
///
/// Takes outer and inner relations (`RelLink`) and joins their rows using
/// a join condition (`ScalarLink`). Scans inner relation for each outer row.
#[derive(Clone)]
pub struct NLJoin<RelLink, ScalarLink> {
    pub join_type: String,
    pub outer: RelLink, // Outer relation.
    pub inner: RelLink, // Inner relation scanned for each outer row.
    pub condition: ScalarLink,
}
