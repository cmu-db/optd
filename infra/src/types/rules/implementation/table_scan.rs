use super::*;
use crate::expression::relational::{logical::LogicalScanExpr, physical::TableScanExpr};

pub struct TableScanRule;

impl ImplementationRule for TableScanRule {
    fn check_and_apply(&self, expr: LogicalExpr) -> Option<PhysicalExpr> {
        if let LogicalExpr::Scan(LogicalScanExpr {}) = expr {
            return Some(PhysicalExpr::TableScan(TableScanExpr {}));
        }

        None
    }
}
