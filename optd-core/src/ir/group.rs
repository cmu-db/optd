/// Unique identifier for a group in the memo structure.
///
/// A group in a query optimizer memo represents a set of logically equivalent
/// expressions. All expressions in a group produce the same result set but may
/// have different physical implementations and costs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GroupId(pub i64);

/// Represents the current exploration status of a group in the memo.
///
/// During query optimization, groups go through different stages of exploration
/// as the optimizer searches for equivalent expressions and transforms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExplorationStatus {
    /// Group has not yet been processed by the explorer
    Unexplored,
    /// Group is currently being processed by the explorer
    /// (used to detect and prevent exploration cycles)
    Exploring,
    /// Group has been fully explored and all applicable rules have been applied
    Explored,
}

/// Represents the cost of a goal / optimized expression in the memo.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(f64);
