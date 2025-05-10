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
