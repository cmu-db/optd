//! Types for logical and physical expressions in the optimizer.

use crate::types::memo::GroupId;
use crate::types::operator::relational::logical::LogicalOperator;
use crate::types::operator::relational::physical::PhysicalOperator;

/// A logical expression in the memo table.
///
/// References children using [`GroupId`]s for expression sharing
/// and memoization.
pub type LogicalExpression = LogicalOperator<GroupId, GroupId>;

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type PhysicalExpression = PhysicalOperator<GroupId, GroupId>;
