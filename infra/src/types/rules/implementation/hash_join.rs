use super::*;
use crate::expression::relational::{logical::LogicalJoinExpr, physical::HashJoinExpr};

pub struct HashJoinRule;

impl ImplementationRule for HashJoinRule {
    fn check_and_apply(&self, expr: LogicalExpr) -> Option<PhysicalExpr> {
        if let LogicalExpr::Join(LogicalJoinExpr {}) = expr {
            return Some(PhysicalExpr::HashJoin(HashJoinExpr {}));
        }

        None
    }
}
