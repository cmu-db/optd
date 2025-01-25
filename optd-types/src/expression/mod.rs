use relation::RelationalExpr;
use scalar::ScalarExpr;

mod relation;
mod scalar;

pub use relation::{LogicalExpr, PhysicalExpr};

/// A type representing an optimization operator in the memo table.
pub enum Expr {
    Relational(RelationalExpr),
    Scalar(ScalarExpr),
}
