//! A logical filter.

/// Logical filter operator that selects rows matching a condition.
///
/// Takes input relation (`Relation`) and filters rows using a boolean predicate (`Scalar`).
#[derive(Clone)]
pub struct Filter<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// The filter expression denoting the predicate condition for this filter operation.
    pub predicate: Scalar,
}
