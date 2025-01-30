//! Memo table implementation for query optimization.
//!
//! The memo table is a core data structure that stores expressions and their logical equivalences
//! during query optimization. It serves two main purposes:
//!
//! - Avoiding redundant optimization by memoizing already explored expressions
//! - Grouping logically equivalent expressions together to enable rule-based optimization
//!
//! # Structure
//!
//! - Each unique expression is assigned an expression ID (either [`LogicalExpressionId`],
//!   [`PhysicalExpressionId`], or [`ScalarExpressionId`])
//! - Logically equivalent expressions are grouped together under a [`GroupId`]
//! - Logically equivalent scalar expressions are grouped toegether under a [`ScalarGroupId`]
//!
//! # Usage
//!
//! The memo table provides methods to:
//! - Add new expressions and get their IDs
//! - Add expressions to existing groups
//! - Retrieve expressions in a group
//! - Look up group membership of expressions
//! - Create new groups for expressions

use crate::expression::LogicalExpression;

/// A unique identifier for a logical expression in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalExpressionId(u64);

/// A unique identifier for a physical expression in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PhysicalExpressionId(u64);

/// A unique identifier for a scalar expression in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScalarExpressionId(u64);

/// A unique identifier for a group of relational expressions in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// A unique identifier for a group of scalar expressions in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScalarGroupId(u64);

/// TODO(alexis) Add fields & link to storage layer.
pub struct Memo;

/// TODO(alexis) Stabilize API by first expanding the Python code.
impl Memo {
    /// TODO(alexis) Add docs.
    pub async fn add_logical_expr_to_group(
        &mut self,
        _group_id: GroupId,
        _logical_expr: LogicalExpression,
    ) -> LogicalExpressionId {
        todo!()
    }
}
