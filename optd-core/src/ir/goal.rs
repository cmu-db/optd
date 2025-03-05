use super::{group::GroupId, properties::PhysicalProperties};

/// A physical optimization goal, consisting of a group to optimize and
/// the required physical properties.
///
/// Goals are used by the optimizer to find the best physical implementation
/// of a logical expression that satisfies specific physical property requirements
/// (like sort order, distribution, etc.).
#[derive(Debug, Clone, PartialEq)]
pub struct Goal(pub GroupId, pub PhysicalProperties);

/// Represents the current optimization status of a goal in the memo.
///
/// During cost-based optimization, goals go through different stages as the
/// optimizer searches for the lowest-cost implementation that satisfies
/// the required physical properties.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OptimizationStatus {
    /// Goal has not yet been processed by the optimizer
    Unoptimized,
    /// Goal is currently being processed by the optimizer
    /// (used to detect and prevent optimization cycles)
    Pending,
    /// Goal has been fully optimized and the best implementation has been found
    Optimized,
}
