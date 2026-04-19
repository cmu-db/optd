use std::sync::Arc;

use datafusion::{
    common::tree_node::Transformed,
    error::DataFusionError,
    logical_expr::LogicalPlan,
    optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};

use crate::OptdExtensionConfig;

#[derive(Debug)]
struct SkipWhenOptdOnlyRule {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
}

impl SkipWhenOptdOnlyRule {
    fn new(inner: Arc<dyn OptimizerRule + Send + Sync>) -> Self {
        Self { inner }
    }
}

impl OptimizerRule for SkipWhenOptdOnlyRule {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.inner.apply_order()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if config
            .options()
            .extensions
            .get::<OptdExtensionConfig>()
            .map(|config| config.optd_only)
            .unwrap_or(false)
        {
            Ok(Transformed::no(plan))
        } else {
            self.inner.rewrite(plan, config)
        }
    }
}

pub fn wrap_logical_optimizer_rules(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    rules
        .into_iter()
        .map(|rule| {
            Arc::new(SkipWhenOptdOnlyRule::new(rule)) as Arc<dyn OptimizerRule + Send + Sync>
        })
        .collect()
}
