use super::{GoalId, LogicalExpressionId, LogicalProperties};
use std::collections::HashSet;

/// A unique identifier for a group in the memo structure.
///
/// A group in a represents a set of logically equivalent expressions. All expressions in a group
/// produce the same result set but may have different physical properties and costs to compute.
///
/// This group takes inspiration from Cascades, but is not exactly the same.
///
/// The major difference between cascades and optd is that a Cascades group is defined as a class of
/// equivalent logical **and** physical expressions. In optd, a group is **only** equivalent logical
/// expressions. optd instead represents physical expressions via [`Goal`]s.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub struct GroupId(pub i64);

/// The representation of a `Group` of logical expressions in the memo table.
pub struct Group {
    /// The logical expression belonging to this `Group`, tracked via [`LogicalExpressionId`].
    pub logical_exprs: HashSet<LogicalExpressionId>,
    /// The logical properties of the group, might be `None` if it hasn't been derived yet.
    pub properties: Option<LogicalProperties>,
    /// The `Goal`s that are dependent on this `Group`.
    pub goals: HashSet<GoalId>,
}
