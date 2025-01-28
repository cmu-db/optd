use crate::types::operator::physical::PhysicalOperator;
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
}
