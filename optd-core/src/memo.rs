//! Memo table implementation for query optimization.
//!
//! The memo table is a core data structure that stores expressions and their logical equivalences
//! during query optimization. It serves two main purposes:
//!
//! - Avoiding redundant optimization by memoizing already explored expressions
//! - Grouping logically equivalent expressions together to enable rule-based optimization
//!
//! # Structure
//! - Each unique expression is assigned an [`ExprId`]
//! - Logically equivalent expressions are grouped together under a [`GroupId`]  
//!
//! # Usage
//! The memo table provides methods to:
//! - Add new expressions and get their IDs
//! - Add expressions to existing groups
//! - Retrieve expressions in a group
//! - Look up group membership of expressions
//! - Create new groups for expressions

use crate::expression::LogicalExpression;

/// A unique identifier for an expression in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExprId(u64);

/// A unique identifier for a group of expressions in the memo table.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO(alexis) Add fields & link to storage layer.
pub struct Memo;

/// TODO(alexis) Stabilize API by first expanding the Python code.
impl Memo {
    pub async fn add_logical_expr_to_group(
        &mut self,
        _logical_expr: LogicalExpression,
        _group_id: GroupId,
    ) -> ExprId {
        todo!()
    }
}
