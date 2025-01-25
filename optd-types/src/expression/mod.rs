mod relational;
mod scalar;

pub use relational::{LogicalExpr, PhysicalExpr, RelationalExpr};
pub use scalar::ScalarExpr;

/// A type representing an optimization operator in the memo table.
pub enum Expr {
    Relational(RelationalExpr),
    Scalar(ScalarExpr),
}
