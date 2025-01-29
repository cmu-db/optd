//! Types for logical and physical expressions in the optimizer.

use crate::memo::GroupId;
use crate::operator::relational::logical::LogicalOperator;
use crate::operator::relational::physical::PhysicalOperator;
use crate::operator::scalar::ScalarOperator;

/// A logical expression in the memo table.
///
/// References children using [`GroupId`]s for expression sharing
/// and memoization.
pub enum LogicalExpression {
    Relational(LogicalOperator<GroupId, GroupId>),
    Scalar(ScalarOperator<GroupId>),
}

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub enum PhysicalExpression {
    Relational(PhysicalOperator<GroupId, GroupId>),
    Scalar(ScalarOperator<GroupId>),
}
