use crate::values::OptdValue;
use serde::Deserialize;

/// Table scan operator that reads rows from a base table
///
/// Reads from table (`String`) and optionally filters rows using
/// a pushdown predicate (`Scalar`).
#[derive(Clone, Debug, Deserialize)]
pub struct TableScan<Value, Scalar> {
    pub table_name: Value,
    pub predicate: Scalar,
}

impl<Scalar> TableScan<OptdValue, Scalar> {
    /// Create a new table scan operator.
    pub fn new(table_name: &str, predicate: Scalar) -> Self {
        Self {
            table_name: OptdValue::String(table_name.to_string()),
            predicate,
        }
    }
}
