//! A physical table scan operator.

use crate::{cascades::ir::OperatorData, operators::relational::physical::PhysicalOperator};
use serde::Deserialize;

/// A physical operator that scans rows from a table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TableScan<Value, Scalar> {
    /// The name of the table to scan.
    pub table_name: Value,
    /// The pushdown predicate.
    pub predicate: Scalar,
}

impl<Scalar> TableScan<OperatorData, Scalar> {
    /// Create a new table scan operator.
    pub fn new(table_name: &str, predicate: Scalar) -> Self {
        Self {
            table_name: OperatorData::String(table_name.to_string()),
            predicate,
        }
    }
}

/// Creates a table scan physical operator.
pub fn table_scan<Relation, Scalar>(
    table_name: &str,
    predicate: Scalar,
) -> PhysicalOperator<OperatorData, Relation, Scalar> {
    PhysicalOperator::TableScan(TableScan::new(table_name, predicate))
}
