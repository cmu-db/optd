/// Table scan operator that reads rows from a base table
///
/// Reads from table (`String`) and optionally filters rows using
/// a pushdown predicate (`Scalar`).
#[derive(Clone)]
pub struct TableScan<Metadata, Scalar> {
    pub table_name: Metadata,
    pub predicate: Option<Scalar>,
}
