//! Memo table interface for query optimization.
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

pub mod persistent_memo;

use serde::{Deserialize, Serialize};

use crate::expression::{LogicalExpression, ScalarExpression};

/// A unique identifier for a group of relational expressions in the memo table.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Serialize, Deserialize,
)]
#[sqlx(transparent)]
pub struct GroupId(pub(super) i64);

/// A unique identifier for a group of scalar expressions in the memo table.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Serialize, Deserialize,
)]
#[sqlx(transparent)]
pub struct ScalarGroupId(pub(super) i64);

/// The exploration status of a group or a logical expression in the memo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(i32)]
pub enum ExplorationStatus {
    /// The group or the logical expression has not been explored.
    Unexplored,
    /// The group or the logical expression is currently being explored.
    Exploring,
    /// The group or the logical expression has been explored.
    Explored,
}

/// The optimization status of a goal or a physical expression in the memo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(i32)]
pub enum OptimizationStatus {
    /// The group or the physical expression has not been optimized.
    Unoptimized,
    /// The group or the physical expression is currently being optimized.
    Pending,
    /// The group or the physical expression has been optimized.
    Optimized,
}

