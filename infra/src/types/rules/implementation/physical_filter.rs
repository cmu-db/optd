use super::*;
use crate::expression::relational::{logical::LogicalFilterExpr, physical::PhysicalFilterExpr};

pub struct PhysicalFilterRule;

impl ImplementationRule for PhysicalFilterRule {
    fn check_and_apply(&self, expr: LogicalExpr) -> Option<PhysicalExpr> {
        if let LogicalExpr::Filter(LogicalFilterExpr {}) = expr {
            return Some(PhysicalExpr::Filter(PhysicalFilterExpr {}));
        }

        None
    }
}
