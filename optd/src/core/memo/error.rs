use crate::core::cir::*;

/// Type alias for results returned by Memoize trait methods
pub type MemoizeResult<T> = Result<T, MemoizeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoizeError {
    /// Error indicating that a group ID was not found in the memo.
    GroupNotFound(GroupId),

    /// Error indicating that a goal ID was not found in the memo.
    GoalNotFound(GoalId),

    /// Error indicating that a logical expression ID was not found in the memo.
    LogicalExprNotFound(LogicalExpressionId),

    /// Error indicating that a physical expression ID was not found in the memo.
    PhysicalExprNotFound(PhysicalExpressionId),

    /// Error indicating that there is no logical expression in the group.
    NoLogicalExprInGroup(GroupId),
}
