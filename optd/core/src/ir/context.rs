use std::sync::Arc;

use crate::ir::{catalog::Catalog, cost::CostModel, properties::CardinalityEstimator};

#[derive(Clone)]
pub struct IRContext {
    /// An accessor to the catalog interface.
    pub cat: Arc<dyn Catalog>,
    /// An accessor to the cardinality estimator.
    pub card: Arc<dyn CardinalityEstimator>,
    /// An accessor to the cost model.
    pub cm: Arc<dyn CostModel>,
}

impl IRContext {
    pub fn new(
        cat: Arc<dyn Catalog>,
        card: Arc<dyn CardinalityEstimator>,
        cost: Arc<dyn CostModel>,
    ) -> Self {
        Self {
            card,
            cat,
            cm: cost,
        }
    }
}
