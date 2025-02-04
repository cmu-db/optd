use serde::Deserialize;

/// Merge join operator that matches rows based on equality conditions.
///
/// Takes sorted left and right relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Both inputs must be sorted on join keys.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct MergeJoin<Value, Relation, Scalar> {
    pub join_type: Value,
    /// Left sorted relation.
    pub left_sorted: Relation,
    /// Right sorted relation.
    pub right_sorted: Relation,
    pub condition: Scalar,
}

impl<Relation, Scalar> MergeJoin<String, Relation, Scalar> {
    /// Create a new merge join operator.
    pub fn new(
        join_type: &str,
        left_sorted: Relation,
        right_sorted: Relation,
        condition: Scalar,
    ) -> Self {
        Self {
            join_type: join_type.into(),
            left_sorted,
            right_sorted,
            condition,
        }
    }
}
