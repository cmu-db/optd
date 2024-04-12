pub mod adaptive_cost;
pub mod base_cost;
mod stats;

pub use adaptive_cost::{AdaptiveCostModel, RuntimeAdaptionStorage, DEFAULT_DECAY};
pub use base_cost::{OptCostModel, COMPUTE_COST, IO_COST, ROW_COUNT};
pub use stats::{BaseTableStats, DataFusionBaseTableStats, DataFusionPerTableStats};

pub trait WithRuntimeStatistics {
    fn get_runtime_statistics(&self) -> RuntimeAdaptionStorage;
}
