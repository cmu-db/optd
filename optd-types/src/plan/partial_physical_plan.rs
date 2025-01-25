use crate::operator::{logical::LogicalOperator, physical::PhysicalOperator, ScalarOperator};
use crate::GroupId;
use std::sync::Arc;

/// TODO Add docs.
pub enum PartialPhysicalPlan {
    LogicalRoot(LogicalLink),
    PhysicalRoot(PhysicalLink),
}

/// TODO Add docs.
pub enum LogicalLink {
    LogicalNode(LogicalOperator<LogicalLink>),
    PhysicalNode(PhysicalOperator<PhysicalLink>),
    ScalarNode(ScalarOperator<ScalarLink>),
    Group(GroupId),
}

/// TODO Add docs.
pub enum PhysicalLink {
    PhysicalNode(PhysicalOperator<PhysicalLink>),
    ScalarNode(ScalarOperator<ScalarLink>),
    Group(GroupId),
}

/// TODO Add docs.
pub enum ScalarLink {
    ScalarNode(ScalarOperator<ScalarLink>),
    Group(GroupId),
}
