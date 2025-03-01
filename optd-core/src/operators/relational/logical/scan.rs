//! A logical scan.

use super::LogicalOperator;
use crate::cascades::ir::OperatorData;
use serde::Deserialize;

/// Logical scan operator that reads from a base table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Scan<Value, Scalar> {
    /// The name of the table to scan.
    pub table_name: Value,
    /// The pushdown predicate.
    pub predicate: Scalar,
}

impl<Scalar> Scan<OperatorData, Scalar> {
    /// Create a new scan operator.
    pub fn new(table_name: &str, predicate: Scalar) -> Self {
        Self {
            table_name: OperatorData::String(table_name.into()),
            predicate,
        }
    }
}

/// Creates a scan logical operator.
pub fn scan<Relation, Scalar>(
    table_name: &str,
    predicate: Scalar,
) -> LogicalOperator<OperatorData, Relation, Scalar> {
    LogicalOperator::Scan(Scan::new(table_name, predicate))
}
