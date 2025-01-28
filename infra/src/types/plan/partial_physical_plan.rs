use crate::types::operator::{
    logical::LogicalOperator, physical::PhysicalOperator, ScalarOperator,
};
use crate::GroupId;
use std::sync::Arc;

/// TODO Add docs.
#[derive(Clone)]
pub enum PartialPhysicalPlan {
    LogicalRoot(LogicalLink),
    PhysicalRoot(PhysicalLink),
}

/// TODO Add docs.
#[derive(Clone)]
pub enum LogicalLink {
    LogicalNode(Arc<LogicalOperator<LogicalLink>>),
    PhysicalNode(Arc<PhysicalOperator<PhysicalLink>>),
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
    Group(GroupId),
}

/// TODO Add docs.
#[derive(Clone)]
pub enum PhysicalLink {
    PhysicalNode(Arc<PhysicalOperator<PhysicalLink>>),
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
    Group(GroupId),
}

/// TODO Add docs.
#[derive(Clone)]
pub enum ScalarLink {
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
    Group(GroupId),
}
