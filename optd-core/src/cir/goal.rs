use super::{group::GroupId, properties::PhysicalProperties};

/// A physical optimization goal, consisting of a group to optimize and
/// the required physical properties.
///
/// Goals are used by the optimizer to find the best physical implementation
/// of a logical expression that satisfies specific physical property requirements
/// (like sort order, distribution, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Goal(pub GroupId, pub PhysicalProperties);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GoalId(pub i64);

/// Represents the cost of a goal / optimized expression in the memo.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);
