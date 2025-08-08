use std::sync::Arc;

use datafusion::{
    execution::{SessionState, context::QueryPlanner},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
};

#[derive(Debug)]
pub struct OptdQueryPlanner;

impl OptdQueryPlanner {
    /// Optd's actual implementation of [`QueryPlanner::create_physical_plan`].
    async fn create_physical_plan_inner(
        &self,
        _logical_plan: &LogicalPlan,
        _session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

impl QueryPlanner for OptdQueryPlanner {
    fn create_physical_plan<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        logical_plan: &'life1 LogicalPlan,
        session_state: &'life2 SessionState,
    ) -> ::core::pin::Pin<
        Box<
            dyn Future<Output = datafusion::common::Result<Arc<dyn ExecutionPlan>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(self.create_physical_plan_inner(logical_plan, session_state))
    }
}
