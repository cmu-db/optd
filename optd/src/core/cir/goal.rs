use super::{PhysicalExpressionId, group::GroupId, properties::PhysicalProperties};
use std::collections::HashSet;
use std::hash::Hash;

/// A physical optimization goal, consisting of a group to optimize and the required physical
/// properties.
///
/// Goals are used by the optimizer to find the best physical implementation of a logical expression
/// that satisfies specific physical property requirements (like sort order, distribution, etc.).
///
/// Goals can be thought of as the physical counterpart to logical [`GroupId`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Goal {
    /// The [`GroupId`] of the group this `Goal` is based on.
    pub group_id: GroupId,
    /// The physical properties of this `Goal`.
    pub properties: PhysicalProperties,
    /// The set of members that are part of this goal.
    pub members: HashSet<GoalMemberId>,
}

/// Represents a member of a goal, which can be either a physical expression
/// or a reference to another goal
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum GoalMemberId {
    /// A physical expression that can satisfy the goal
    PhysicalExpressionId(PhysicalExpressionId),

    /// A reference to another goal that can satisfy this goal
    GoalId(GoalId),
}

/// A unique identifier for a goal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct GoalId(pub i64);

/// Represents the cost of a goal / optimized expression in the memo.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);
