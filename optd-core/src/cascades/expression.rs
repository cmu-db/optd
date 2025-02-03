//! Types for logical and physical expressions in the optimizer.

use crate::{
    operators::relational::{logical::LogicalOperator, physical::PhysicalOperator},
    types::OptdType,
};

use super::memo::GroupId;

/// A logical expression in the memo table.
///
/// References children using [`GroupId`]s for expression sharing
/// and memoization.
pub type LogicalExpression = LogicalOperator<OptdType, GroupId, GroupId>;

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type PhysicalExpression = PhysicalOperator<OptdType, GroupId, GroupId>;
