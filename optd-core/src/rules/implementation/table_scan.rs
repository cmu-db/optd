use crate::operator::relational::{
    logical::LogicalOperator,
    physical::{scan::table_scan::TableScan, PhysicalOperator},
};

use super::*;

// Implementation rule that converts a logical scan into a table scan physical operator.
pub struct TableScanRule;

impl ImplementationRule for TableScanRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        if let LogicalExpression::Relational(LogicalOperator::Scan(scan)) = expr {
            return Some(PhysicalExpression::Relational(PhysicalOperator::TableScan(
                TableScan {
                    table_name: scan.table_name,
                    predicate: scan.predicate,
                },
            )));
        }

        None
    }
}
