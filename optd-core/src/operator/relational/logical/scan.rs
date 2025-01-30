//! A logical scan.

/// Logical scan operator that reads from a base table.
///
/// Reads from table (`String`) and optionally filters rows using a pushdown predicate
/// (`Scalar`).
#[derive(Clone)]
pub struct Scan<Scalar> {
    /// TODO(alexis) Mocked for now.
    pub table_name: String,
    /// An optional filter expression for predicate pushdown into scan operators.
    pub predicate: Option<Scalar>,
}
