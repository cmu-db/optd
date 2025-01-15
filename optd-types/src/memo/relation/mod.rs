mod logical_expression;
pub use logical_expression::LogicalExpression;

mod physical_expression;
pub use physical_expression::PhysicalExpression;

/// A type representing logical or physical operators in a relational algebraic query plan.
pub enum Relation {
    Logical(LogicalExpression),
    Physical(PhysicalExpression),
}
