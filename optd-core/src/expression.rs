//! Types for logical and physical expressions in the optimizer.

use crate::memo::{GroupId, ScalarGroupId};
use crate::operator::relational::logical::LogicalOperator;
use crate::operator::relational::physical::PhysicalOperator;
use crate::operator::scalar::ScalarOperator;

/// A logical expression in the memo table.
///
/// References children using [`GroupId`]s for expression sharing and memoization.
pub type LogicalExpression = LogicalOperator<GroupId, ScalarGroupId>;

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation strategies.
pub type PhysicalExpression = PhysicalOperator<GroupId, ScalarGroupId>;

/// A scalar expression in the memo table.
///
/// References children using [`ScalarGroupId`]s for expression sharing and memoization.
pub type ScalarExpression = ScalarOperator<ScalarGroupId>;
