use crate::operator::relational::logical::LogicalOperator;
use crate::operator::relational::physical::PhysicalOperator;
use crate::operator::scalar::ScalarOperator;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExprId(u64);

// TODO: Put code somwehere else.
type ScalarGroupId = u64;
type RelGroupId = u64;

/// A type representing a logical expression in the memo table.
pub enum LogicalExpression {
    Relational(LogicalOperator<RelGroupId, ScalarGroupId>),
    Scalar(ScalarOperator<ScalarGroupId>),
}

/// A type representing a physical expression in the memo table.
pub enum PhysicalExpression {
    Relational(PhysicalOperator<RelGroupId, ScalarGroupId>),
    Scalar(ScalarOperator<ScalarGroupId>),
}
