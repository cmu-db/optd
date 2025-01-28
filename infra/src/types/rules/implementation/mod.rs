use crate::expression::relational::{logical::LogicalExpr, physical::PhysicalExpr};

#[allow(dead_code)]
pub trait ImplementationRule {
    /// TODO Is this a correct interface?
    /// TODO Add docs.
    fn check_and_apply(&self, expr: LogicalExpr) -> Option<PhysicalExpr>;
}

pub mod hash_join;
pub mod physical_filter;
pub mod table_scan;
