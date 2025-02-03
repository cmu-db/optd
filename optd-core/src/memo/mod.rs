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

use serde::Deserialize;

use crate::expression::LogicalExpression;

/// A unique identifier for a logical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct LogicalExpressionId(pub(super) i64);

/// A unique identifier for a physical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct PhysicalExpressionId(pub(super) i64);

/// A unique identifier for a scalar expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ScalarExpressionId(pub(super) i64);

/// A unique identifier for a group of relational expressions in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct GroupId(pub(super) i64);

/// A unique identifier for a group of scalar expressions in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
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

/// A trait for memoizing expressions.
#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    /// Gets the exploration status of a group.
    async fn get_group_exploration_status(
        &self,
        group_id: GroupId,
    ) -> anyhow::Result<ExplorationStatus>;

    /// Sets the exploration status of a group.
    async fn set_group_exploration_status(
        &self,
        group_id: GroupId,
        status: ExplorationStatus,
    ) -> anyhow::Result<()>;

    /// Gets all logical expressions in a group.
    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: GroupId,
    ) -> anyhow::Result<Vec<LogicalExpression>>;

    /// Adds a logical expression to a group.
    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: GroupId,
    ) -> anyhow::Result<GroupId>;

    /// Adds a logical expression.
    async fn add_logical_expr(&self, logical_expr: &LogicalExpression) -> anyhow::Result<GroupId>;

    // /// Gets the optimization status of a goal.
    // async fn get_goal_optimization_status(
    //     &self,
    //     group_id: GroupId,
    //     required_phys_props: (),
    // ) -> OptimizationStatus;

    // /// Sets the optimization status of a goal.
    // async fn set_goal_optimization_status(
    //     &self,
    //     group_id: GroupId,
    //     required_phys_props: (),
    //     status: OptimizationStatus,
    // );

    // // TODO(yuchen): return type.
    // /// Gets the winner of a goal.
    // async fn get_goal_optimization_winner(&self, group_id: GroupId);

    // /// Sets the winner of a goal.
    // async fn set_goal_optimization_winner(&self, group_id: GroupId, required_phys_props: ());

    // /// Checks if a transformation has been applied to a group.
    // async fn is_transformation_applied(
    //     &self,
    //     logical_expr_id: LogicalExpressionId,
    //     transformation_rule_id: u64,
    // ) -> bool;

    // /// Marks a transformation as applied to a group.
    // async fn mark_transformation_applied(
    //     &self,
    //     logical_expr_id: LogicalExpressionId,
    //     transformation_rule_id: u64,
    // );

    // /// Gets the group id of a logical expression.
    // async fn get_relation_group(&self, logical_expr: LogicalExpression) -> GroupId;

    // /// Adds a physical expression to a group.
    // async fn add_physical_expr_to_group(
    //     &mut self,
    //     group_id: GroupId,
    //     physical_expr: PhysicalExpression,
    // ) -> PhysicalExpressionId;

    // /// Sets the optimization status of a physical expression.
    // async fn set_physical_expr_optimization_status(
    //     &mut self,
    //     group_id: GroupId,
    //     physical_expr_id: PhysicalExpressionId,
    //     status: OptimizationStatus,
    // );

    // /// Sets the winner of a physical expression.
    // async fn set_physical_expr_winner(
    //     group_id: GroupId,
    //     winner_physical_expr_id: PhysicalExpressionId,
    //     cost: f64,
    //     winner_statistics: (),
    // );

    // async fn get_goal_winner(&self, group_id: GroupId) -> PhysicalExpressionId;
}
