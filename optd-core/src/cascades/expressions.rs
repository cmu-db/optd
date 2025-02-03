//! Types for logical and physical expressions in the optimizer.

use crate::{
    operators::{
        relational::{logical::LogicalOperator, physical::PhysicalOperator},
        scalar::ScalarOperator,
    },
    types::OptdType,
};

use super::memo::{RelationalGroupId, ScalarGroupId};

/// A logical expression in the memo table.
///
/// References children using [`GroupId`]s for expression sharing
/// and memoization.
pub type LogicalExpression = LogicalOperator<OptdType, RelationalGroupId, ScalarGroupId>;
pub type LogicalExpressionId = i64;

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type PhysicalExpression = PhysicalOperator<OptdType, RelationalGroupId, ScalarGroupId>;
pub type LogicalExpressionId = i64;

/// A scalar expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type ScalarExpression = ScalarOperator<OptdType, ScalarGroupId>;
pub type ScalarExpressionId = i64;
