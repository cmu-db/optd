//! A logical projection.

/// Logical project operator that specifies output columns.
///
/// Takes input relation (`Relation`) and defines output columns/expressions
/// (`Scalar`).
#[derive(Clone)]
pub struct Project<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// TODO(everyone): What exactly is going on here?
    pub fields: Vec<Scalar>,
}
