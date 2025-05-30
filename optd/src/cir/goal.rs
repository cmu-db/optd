use super::{PhysicalExpressionId, group::GroupId, properties::PhysicalProperties};

/// A physical optimization goal, consisting of a group to optimize and the required physical
/// properties.
///
/// Goals are used by the optimizer to find the best physical implementation of a logical expression
/// that satisfies specific physical property requirements (like sort order, distribution, etc.).
///
/// Goals can be thought of as the physical counterpart to logical [`GroupId`]s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Goal(pub GroupId, pub PhysicalProperties);

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
