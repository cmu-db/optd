mod adv_cost;

pub trait WithRuntimeStatistics {
    fn get_runtime_statistics(&self) -> RuntimeAdaptionStorage;
}
