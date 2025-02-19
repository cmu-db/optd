use serde::Deserialize;

/// A unique identifier for a group of relational expressions in the memo table.
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
pub struct RelationalGroupId(pub i64);

/// A unique identifier for a group of scalar expressions in the memo table.
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
pub struct ScalarGroupId(pub i64);

/// The exploration status of a group or a logical expression in the memo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
pub enum ExplorationStatus {
    /// The group or the logical expression has not been explored.
    Unexplored,
    /// The group or the logical expression is currently being explored.
    Exploring,
    /// The group or the logical expression has been explored.
    Explored,
}
