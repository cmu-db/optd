/// Logical scan operator that reads from a base table.
///
/// Reads from table (`String`) and optionally filters rows using a pushdown predicate
/// (`Scalar`).
#[derive(Clone)]
pub struct Scan<Scalar> {
    pub table_name: String, // TODO(alexis): Mocked for now.
    pub predicate: Option<Scalar>,
}
