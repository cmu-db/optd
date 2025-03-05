//! Memo table interface for query optimization.
//!
//! The memo table is a core data structure that stores expressions and their logical equivalences
//! during query optimization. It serves two main purposes:
//!
//! - Avoiding redundant optimization by memoizing already explored expressions
//! - Grouping logically equivalent expressions together to enable rule-based optimization
//!

use crate::ir::{
    expressions::{
        LogicalExpression, LogicalExpressionId, PhysicalExpression, PhysicalExpressionId,
    },
    goal::{Goal, OptimizationStatus},
    group::{Cost, ExplorationStatus, GroupId},
    properties::PhysicalProperties,
};
use anyhow::Result;
use std::sync::Arc;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    /// Creates or get an optimization goal for a group with some required physical properties.
    async fn create_or_get_goal(
        &self,
        group_id: GroupId,
        required_physical_props: &PhysicalProperties,
    ) -> Result<Goal>;

    async fn update_goal_optimization_status(
        &self,
        goal: Goal,
        status: OptimizationStatus,
    ) -> Result<()>;

    async fn get_group_exploration_status(&self, group_id: GroupId) -> Result<ExplorationStatus>;

    async fn update_group_exploration_status(
        &self,
        group_id: GroupId,
        status: ExplorationStatus,
    ) -> Result<()>;

    async fn get_group_optimization_status(&self, goal: Goal) -> Result<OptimizationStatus>;

    /// Gets the metadata that describes a goal.
    async fn get_goal_details(
        &self,
        goal: Goal,
    ) -> Result<(PhysicalProperties, OptimizationStatus, GroupId)>;

    /// Gets all logical expressions in a group.
    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<(LogicalExpressionId, LogicalExpression)>>;

    /// Adds a logical expression to an existing group.
    /// Returns the group id of new group if merge happened.
    // TODO: need to indicate if a new logical expression is added.
    // Option<LogicalExpressionId>
    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: GroupId,
    ) -> Result<GroupId>;

    /// Adds a logical expression to the memo table.
    /// Returns the group id of group that the logical expression is added to. It might create a new group if necessary.
    /// Returns the logical expression id of the logical expression that has just been added.
    async fn add_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<(GroupId, LogicalExpressionId)>;

    /// Merges two relational groups and returns the new group id.
    async fn merge_relation_group(&self, from: GroupId, to: GroupId) -> Result<GroupId>;

    /// Gets the winner physical expression.
    async fn get_winner_physical_expr_in_goal(
        &self,
        goal: Goal,
    ) -> Result<Option<(PhysicalExpressionId, Arc<PhysicalExpression>, Cost)>>;

    /// Gets all physical expressions in a goal.
    async fn get_all_physical_exprs_in_goal(
        &self,
        goal: Goal,
    ) -> Result<Vec<(PhysicalExpressionId, Arc<PhysicalExpression>)>>;

    /// Adds a physical expression to a goal in the memo table.
    // TODO: cost and statistics probably is also added here.
    async fn add_physical_expr_to_goal(
        &self,
        physical_expr: &PhysicalExpression,
        cost: Cost,
        goal: Goal,
    ) -> Result<(Goal, PhysicalExpressionId)>;

    /// Gets the group id from a logical expression. If the logical expression is not in the memo table, it will be created and added to the memo table and the group id will be returned.
    async fn get_group_id_from_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<GroupId>;

    /// Gets the goal id from a physical expression. If the physical expression is not in the memo table, it will be created and added to the memo table and the goal id will be returned.
    async fn get_goal_from_physical_expr(
        &self,
        physical_expr: &PhysicalExpression,
        properties: &PhysicalProperties,
    ) -> Result<Goal>;

    async fn get_repr_group(&self, group_id: &GroupId) -> Result<GroupId>;

    async fn get_repr_goal(&self, goal: &Goal) -> Result<Goal>;
}
