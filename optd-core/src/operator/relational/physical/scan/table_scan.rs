/// Table scan operator that reads rows from a base table
///
/// Reads from table (`String`) and optionally filters rows using
/// a pushdown predicate (`ScalarLink`).
#[derive(Clone)]
pub struct TableScan<ScalarLink> {
    pub table_name: String, // TODO(alexis): Mocked for now.
    pub predicate: Option<ScalarLink>,
}
