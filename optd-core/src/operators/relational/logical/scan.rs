//! A logical scan.

use serde::Deserialize;

use crate::values::OptdValue;

/// Logical scan operator that reads from a base table.
///
/// Reads from table (`String`) and optionally filters rows using a pushdown predicate
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Scan<Value, Scalar> {
    pub table_name: Value,
    /// An optional filter expression for predicate pushdown into scan operators.
    ///
    /// For example, a `Filter(Scan(A), column_a < 42)` can be converted into a predicate pushdown
    /// `Scan(A, column < 42)` to prevent having to materialize many tuples.
    pub predicate: Scalar,
}

impl<Scalar> Scan<OptdValue, Scalar> {
    /// Create a new scan operator
    pub fn new(table_name: &str, predicate: Scalar) -> Self {
        Self {
            table_name: OptdValue::String(table_name.into()),
            predicate,
        }
    }
}
