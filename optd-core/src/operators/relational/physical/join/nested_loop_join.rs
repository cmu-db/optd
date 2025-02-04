use serde::Deserialize;

/// Nested-loop join operator that matches rows based on a predicate.
///
/// Takes outer and inner relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Scans inner relation for each outer row.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct NestedLoopJoin<Value, Relation, Scalar> {
    pub join_type: Value,
    /// Outer relation.
    pub outer: Relation,
    /// Inner relation scanned for each outer row.
    pub inner: Relation,
    pub condition: Scalar,
}

impl<Relation, Scalar> NestedLoopJoin<String, Relation, Scalar> {
    /// Create a new nested-loop join operator.
    pub fn new(join_type: &str, outer: Relation, inner: Relation, condition: Scalar) -> Self {
        Self {
            join_type: join_type.into(),
            outer,
            inner,
            condition,
        }
    }
}
