use super::context::OptdDFContext;
use datafusion::logical_expr::LogicalPlan as DataFusionLogicalPlan;
use optd_core::cir::*;

impl OptdDFContext {
    /// Given a DataFusion logical plan, returns an `optd` [`LogicalPlan`].
    pub(crate) fn df_to_optd_relational(
        &mut self,
        _df_logical_plan: &DataFusionLogicalPlan,
    ) -> anyhow::Result<LogicalPlan> {
        todo!("implement conversion from datafusion to optd")
    }
}
