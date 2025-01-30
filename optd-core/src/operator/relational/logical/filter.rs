//! A logical filter.

/// Logical filter operator that selects rows matching a condition.
///
/// Takes input relation (`Relation`) and filters rows using a boolean predicate (`Scalar`).
#[derive(Clone)]
pub struct Filter<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// The filter expression denoting the predicate condition for this filter operation.
    ///
    /// For example, a filter predicate could be `column_a > 42`, or it could be something like
    /// `column_b < 100 AND column_c > 1000`.
    pub predicate: Scalar,
}
