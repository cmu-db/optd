//! The rule for implementing `Scan` as a `TableScan`.
//!
//! See [`TableScanRule`] for more information.

use crate::operator::relational::{
    logical::{scan::Scan, LogicalOperator},
    physical::{scan::table_scan::TableScan, PhysicalOperator},
};

use super::*;

/// A unit / marker struct for implementing `TableScan`.
///
/// This implementation rule converts a logical `Scan` into a physical `TableScan` operator.
pub struct TableScanRule;

impl ImplementationRule for TableScanRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        let LogicalOperator::Scan(Scan {
            table_name,
            predicate,
            ..
        }) = expr
        else {
            return None;
        };

        Some(PhysicalOperator::TableScan(TableScan::new(
            &table_name,
            predicate,
        )))
    }
}
