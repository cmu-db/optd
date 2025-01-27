use crate::types::operator::{logical::LogicalOperator, physical::PhysicalOperator, ScalarOperator};
use std::sync::Arc;

/// TODO Add docs.
#[derive(Clone)]
pub enum PhysicalPlan {
    LogicalRoot(LogicalLink),
    PhysicalRoot(PhysicalLink),
}

/// TODO Add docs.
#[allow(clippy::enum_variant_names)]
#[derive(Clone)]
pub enum LogicalLink {
    LogicalNode(Arc<LogicalOperator<LogicalLink>>),
    PhysicalNode(Arc<PhysicalOperator<PhysicalLink>>),
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
}

/// TODO Add docs.
#[derive(Clone)]
pub enum PhysicalLink {
    PhysicalNode(Arc<PhysicalOperator<PhysicalLink>>),
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
}

/// TODO Add docs.
#[derive(Clone)]
pub enum ScalarLink {
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
}
