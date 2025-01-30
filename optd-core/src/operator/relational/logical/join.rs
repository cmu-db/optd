//! A logical join.

/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Clone)]
pub struct Join<Relation, Scalar> {
    /// TODO(alexis) Mocked for now.
    pub join_type: String,
    /// The left input relation.
    pub left: Relation,
    /// The right input relation.
    pub right: Relation,
    /// The join expression denoting the join condition that links the two input relations.
    pub condition: Scalar,
}
