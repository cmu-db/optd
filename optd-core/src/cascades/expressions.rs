//! Types for logical and physical expressions in the optimizer.

use crate::{
    operators::{
        relational::{logical::LogicalOperator, physical::PhysicalOperator},
        scalar::ScalarOperator,
    },
    values::OptdValue,
};

use super::{RelationalGroupId, ScalarGroupId};

/// A logical expression in the memo table.
///
/// References children using [`GroupId`]s for expression sharing
/// and memoization.
pub type LogicalExpression = LogicalOperator<OptdValue, RelationalGroupId, ScalarGroupId>;

#[derive(Debug, Clone, Copy)]
pub struct LogicalExpressionId(pub i64);

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type PhysicalExpression = PhysicalOperator<OptdValue, RelationalGroupId, ScalarGroupId>;

#[derive(Debug, Clone, Copy)]
pub struct PhysicalExpressionId(pub i64);

/// A scalar expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type ScalarExpression = ScalarOperator<OptdValue, ScalarGroupId>;

#[derive(Debug, Clone, Copy)]
pub struct ScalarExpressionId(pub i64);
