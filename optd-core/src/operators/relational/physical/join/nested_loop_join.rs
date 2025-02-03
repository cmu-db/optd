/// Nested-loop join operator that matches rows based on a predicate.
///
/// Takes outer and inner relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Scans inner relation for each outer row.
#[derive(Clone)]
pub struct NestedLoopJoin<Metadata, Relation, Scalar> {
    pub join_type: Metadata,
    /// Outer relation.
    pub outer: Relation,
    /// Inner relation scanned for each outer row.
    pub inner: Relation,
    pub condition: Scalar,
}
