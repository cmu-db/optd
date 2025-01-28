use crate::operator::{physical::PhysicalOperator, ScalarOperator};
use std::sync::Arc;

/// TODO Add docs.
#[derive(Clone)]
pub struct PhysicalPlan {
    pub root: Arc<PhysicalOperator<PhysicalLink>>,
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
