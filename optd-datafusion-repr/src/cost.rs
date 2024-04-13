pub mod adaptive_cost;
pub mod base_cost;

pub use adaptive_cost::{AdaptiveCostModel, RuntimeAdaptionStorage, DEFAULT_DECAY};
pub use base_cost::stats::{BaseTableStats, DataFusionBaseTableStats, DataFusionPerTableStats};
pub use base_cost::{OptCostModel, COMPUTE_COST, IO_COST, ROW_COUNT};

pub trait WithRuntimeStatistics {
    fn get_runtime_statistics(&self) -> RuntimeAdaptionStorage;
}
