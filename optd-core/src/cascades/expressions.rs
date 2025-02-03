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

/// A physical expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type PhysicalExpression = PhysicalOperator<OptdType, RelationalGroupId, ScalarGroupId>;

/// A scalar expression in the memo table.
///
/// Like [`LogicalExpression`] but with specific implementation
/// strategies.
pub type ScalarExpression = ScalarOperator<OptdType, ScalarGroupId>;
