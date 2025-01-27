pub mod relational;
pub mod scalar;

/// A type representing an optimization operator in the memo table.
pub enum Expr {
    Relational(relational::RelationalExpr),
    Scalar(scalar::ScalarExpr),
}
