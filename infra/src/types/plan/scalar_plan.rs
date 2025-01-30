use std::sync::Arc;

use crate::types::operator::scalar::ScalarOperator;

/// A representation of a scalar query plan DAG (directed acyclic graph).
#[derive(Clone)]
pub struct ScalarPlan {
    pub node: Arc<ScalarOperator<ScalarPlan>>,
}
