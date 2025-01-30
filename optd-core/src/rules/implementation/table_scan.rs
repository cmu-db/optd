use crate::operator::relational::{
    logical::LogicalOperator,
    physical::{scan::table_scan::TableScan, PhysicalOperator},
};

use super::*;

// Implementation rule that converts a logical scan into a table scan physical operator.
pub struct TableScanRule;

impl ImplementationRule for TableScanRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        if let LogicalOperator::Scan(scan) = expr {
            return Some(PhysicalOperator::TableScan(TableScan::new(
                scan.table_name,
                scan.predicate,
            )));
        }

        None
    }
}
