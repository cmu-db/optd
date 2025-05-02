use crate::core::cir::*;

/// A type alias for results returned by the different memo table trait methods.
///
/// See the private `traits.rs` module for more information (note that the traits are re-exported).
pub type MemoResult<T> = Result<T, MemoError>;

/// The possible kinds of errors that memo table and task graph state operations can run into.
#[derive(Debug, Clone, Copy)]
pub enum MemoError {
    /// A [`GroupId`] does not exist in the memo.
    GroupNotFound(GroupId),

    /// A [`GoalId`] does not exist in the memo.
    GoalNotFound(GoalId),

    /// A [`LogicalExpressionId`] does not exist in the memo.
    LogicalExprNotFound(LogicalExpressionId),

    /// A [`PhysicalExpressionId`] does not exist in the memo.
    PhysicalExprNotFound(PhysicalExpressionId),

    /// A group does not contain any logical expressions.
    NoLogicalExprInGroup(GroupId),
}
