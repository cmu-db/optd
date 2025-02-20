use serde::Deserialize;

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
#[repr(i32)]
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
    pub id: GoalId,
    pub optimization_status: OptimizationStatus,
}
