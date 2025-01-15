mod logical;
pub use logical::LogicalExpression;

mod physical;
pub use physical::PhysicalExpression;

/// A type representing logical or physical expressions in the memo table.
pub enum RelationExpression {
    LogicalExpression(LogicalExpression),
    PhysicalExpression(PhysicalExpression),
}
