/// Unique identifier for a group in the memo structure.
///
/// A group in a query optimizer memo represents a set of logically equivalent
/// expressions. All expressions in a group produce the same result set but may
/// have different physical implementations and costs.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub struct GroupId(pub i64);
