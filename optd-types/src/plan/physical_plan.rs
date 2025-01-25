use crate::operator::{logical::LogicalOperator, physical::PhysicalOperator, ScalarOperator};
use std::sync::Arc;

/// TODO Add docs.
pub enum PhysicalPlan {
    LogicalRoot(LogicalLink),
    PhysicalRoot(PhysicalLink),
}

/// TODO Add docs.
#[allow(clippy::enum_variant_names)]
pub enum LogicalLink {
    LogicalNode(LogicalOperator<LogicalLink>),
    PhysicalNode(PhysicalOperator<PhysicalLink>),
    ScalarNode(ScalarOperator<ScalarLink>),
}

/// TODO Add docs.
pub enum PhysicalLink {
    PhysicalNode(PhysicalOperator<PhysicalLink>),
    ScalarNode(ScalarOperator<ScalarLink>),
}

/// TODO Add docs.
pub enum ScalarLink {
    ScalarNode(ScalarOperator<ScalarLink>),
}
