use super::context::OptdDFContext;
use async_recursion::async_recursion;
use datafusion::physical_plan::ExecutionPlan;
use optd_core::cir::*;
use std::sync::Arc;

impl OptdDFContext {
    /// Converts an `optd` [`PhysicalPlan`] into an executable DataFusion [`ExecutionPlan`].
    #[async_recursion]
    pub(crate) async fn optd_to_df_relational(
        &self,
        _optimized_plan: &PhysicalPlan,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        todo!("Implement conversion from optd to datafusion")
    }
}
