use relation::RelationExpression;
use scalar::ScalarExpression;

mod relation;
mod scalar;

pub use relation::{LogicalExpression, PhysicalExpression};

/// A type representing an optimization operator in the memo table.
pub enum Expression {
    RelationExpression(RelationExpression),
    ScalarExpression(ScalarExpression),
}
