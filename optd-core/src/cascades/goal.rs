use std::sync::Arc;

use serde::Deserialize;

use super::{groups::RelationalGroupId, properties::PhysicalProperties};

/// A unique identifier for a goal in the memo table.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    sqlx::Type,
    serde::Serialize,
    Deserialize,
)]
#[sqlx(transparent)]
pub struct GoalId(pub i64);

/// The optimization status of a group or a physical expression with a goal in the memo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
pub enum OptimizationStatus {
    /// The group or the physical expression has not been explored.
    Unoptimized,
    /// The group or the physical expression is currently being explored.
    Pending,
    /// The group or the physical expression has been explored.
    Optimized,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Goal {
    /// The identifier of the goal.
    pub representative_goal_id: GoalId,
    /// The identifier of the group that the goal is associated with.
    pub group_id: RelationalGroupId,
    /// The required physical properties for the goal.
    #[sqlx(json)]
    pub required_physical_properties: Arc<PhysicalProperties>,
    /// The optimization status of the goal.
    pub optimization_status: OptimizationStatus,
}
