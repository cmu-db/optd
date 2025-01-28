pub mod logical;
pub mod physical;

/// A type representing logical or physical expressions in the memo table.
pub enum RelationalExpr {
    Logical(logical::LogicalExpr),
    Physical(physical::PhysicalExpr),
}
