//! Memo table interface for query optimization.
//!
//! The memo table is a core data structure that stores expressions and their logical equivalences
//! during query optimization. It serves two main purposes:
//!
//! - Avoiding redundant optimization by memoizing already explored expressions
//! - Grouping logically equivalent expressions together to enable rule-based optimization
//!

use crate::ir::{
    cost::Cost,
    expressions::{
        LogicalExpression, LogicalExpressionId, PhysicalExpression, PhysicalExpressionId,
        ScalarExpression, ScalarExpressionId,
    },
    goal::{OptimizationStatus, PhysicalGoal},
    groups::{ExplorationStatus, LogicalGroupId, ScalarGroupId},
    properties::PhysicalProperties,
};
use anyhow::Result;
use std::sync::Arc;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    /// Creates or get an optimization goal for a group with some required physical properties.
    async fn create_or_get_goal(
        &self,
        group_id: LogicalGroupId,
        required_physical_props: &PhysicalProperties,
    ) -> Result<PhysicalGoal>;

    async fn update_goal_optimization_status(
        &self,
        goal: PhysicalGoal,
        status: OptimizationStatus,
    ) -> Result<()>;

    async fn get_group_exploration_status(
        &self,
        group_id: LogicalGroupId,
    ) -> Result<ExplorationStatus>;

    async fn update_group_exploration_status(
        &self,
        group_id: LogicalGroupId,
        status: ExplorationStatus,
    ) -> Result<()>;

    async fn get_group_optimization_status(&self, goal: PhysicalGoal)
        -> Result<OptimizationStatus>;

    /// Gets the metadata that describes a goal.
    async fn get_goal_details(
        &self,
        goal: PhysicalGoal,
    ) -> Result<(PhysicalProperties, OptimizationStatus, LogicalGroupId)>;

    /// Gets all logical expressions in a group.
    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: LogicalGroupId,
    ) -> Result<Vec<(LogicalExpressionId, LogicalExpression)>>;

    /// Adds a logical expression to an existing group.
    /// Returns the group id of new group if merge happened.
    // TODO: need to indicate if a new logical expression is added.
    // Option<LogicalExpressionId>
    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: LogicalGroupId,
    ) -> Result<LogicalGroupId>;

    /// Adds a logical expression to the memo table.
    /// Returns the group id of group that the logical expression is added to. It might create a new group if necessary.
    /// Returns the logical expression id of the logical expression that has just been added.
    async fn add_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<(LogicalGroupId, LogicalExpressionId)>;

    /// Gets all scalar expressions in a group.
    async fn get_all_scalar_exprs_in_group(
        &self,
        group_id: ScalarGroupId,
    ) -> Result<Vec<(ScalarExpressionId, Arc<ScalarExpression>)>>;

    /// Adds a scalar expression to an existing group.
    /// Returns the group id of new group if merge happened.
    async fn add_scalar_expr_to_group(
        &self,
        scalar_expr: &ScalarExpression,
        group_id: ScalarGroupId,
    ) -> Result<ScalarGroupId>;

    /// Adds a scalar expression to the memo table.
    /// Returns the group id of group if already exists, otherwise creates a new group.
    async fn add_scalar_expr(
        &self,
        scalar_expr: &ScalarExpression,
    ) -> Result<(ScalarGroupId, ScalarExpressionId)>;

    /// Merges two relational groups and returns the new group id.
    async fn merge_relation_group(
        &self,
        from: LogicalGroupId,
        to: LogicalGroupId,
    ) -> Result<LogicalGroupId>;

    /// Merges two scalar groups and returns the new group id.
    async fn merge_scalar_group(
        &self,
        from: ScalarGroupId,
        to: ScalarGroupId,
    ) -> Result<ScalarGroupId>;

    async fn merge_goal(&self, from: PhysicalGoal, to: PhysicalGoal) -> Result<PhysicalGoal>;

    /// Gets the winner physical expression.
    async fn get_winner_physical_expr_in_goal(
        &self,
        goal: PhysicalGoal,
    ) -> Result<Option<(PhysicalExpressionId, Arc<PhysicalExpression>, Cost)>>;

    /// Gets all physical expressions in a goal.
    async fn get_all_physical_exprs_in_goal(
        &self,
        goal: PhysicalGoal,
    ) -> Result<Vec<(PhysicalExpressionId, Arc<PhysicalExpression>)>>;

    /// Adds a physical expression to a goal in the memo table.
    // TODO: cost and statistics probably is also added here.
    async fn add_physical_expr_to_goal(
        &self,
        physical_expr: &PhysicalExpression,
        cost: Cost,
        goal: PhysicalGoal,
    ) -> Result<(PhysicalGoal, PhysicalExpressionId)>;

    /// Gets the group id from a logical expression. If the logical expression is not in the memo table, it will be created and added to the memo table and the group id will be returned.
    async fn get_group_id_from_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<LogicalGroupId>;

    /// Gets the goal id from a physical expression. If the physical expression is not in the memo table, it will be created and added to the memo table and the goal id will be returned.
    async fn get_goal_from_physical_expr(
        &self,
        physical_expr: &PhysicalExpression,
        properties: &PhysicalProperties,
    ) -> Result<PhysicalGoal>;

    async fn get_repr_group(&self, group_id: &LogicalGroupId) -> Result<LogicalGroupId>;

    async fn get_repr_goal(&self, goal: &PhysicalGoal) -> Result<PhysicalGoal>;
}
