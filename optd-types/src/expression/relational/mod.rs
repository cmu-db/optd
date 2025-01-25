mod logical;
pub use logical::LogicalExpr;

mod physical;
pub use physical::PhysicalExpr;

/// A type representing logical or physical expressions in the memo table.
pub enum RelationalExpr {
    Logical(LogicalExpr),
    Physical(PhysicalExpr),
}
