use std::sync::Arc;

use adaptive_cost::{AdaptiveCostModel, RuntimeAdaptionStorage, DEFAULT_DECAY};
use adv_cost::stats::DataFusionBaseTableStats;

use optd_datafusion_repr::{properties::schema::Catalog, DatafusionOptimizer};

pub mod adaptive_cost;
pub mod adv_cost;

pub trait WithRuntimeStatistics {
    fn get_runtime_statistics(&self) -> RuntimeAdaptionStorage;
}

pub fn new_physical_adv_cost(
    catalog: Arc<dyn Catalog>,
    stats: DataFusionBaseTableStats,
    enable_adaptive: bool,
) -> DatafusionOptimizer {
    let cost_model = AdaptiveCostModel::new(DEFAULT_DECAY, stats);
    let runtime_map = cost_model.get_runtime_map();
    DatafusionOptimizer::new_physical_with_cost_model(
        catalog,
        enable_adaptive,
        cost_model,
        runtime_map,
    )
}
