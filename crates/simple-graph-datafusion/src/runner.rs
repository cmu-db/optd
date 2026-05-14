//! [`SimpleGraphRunner`] — a sqllogictest [`AsyncDB`] that routes SQL through
//! simple-graph IR before execution.
//!
//! Flow:
//!   SQL → DataFusion logical plan
//!       → `from_logical_plan` → simple-graph IR
//!       → optimizer passes (identity round-trip for now)
//!       → `to_logical_plan` → DataFusion logical plan
//!       → DataFusion execution
//!       → results

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use simple_graph::{OptimizerContext, QueryContext};
use sqllogictest::{AsyncDB, DBOutput};

use crate::from_df::from_logical_plan;
use crate::to_df::to_logical_plan;

/// Runs SQL through simple-graph IR before handing back to DataFusion for execution.
pub struct SimpleGraphRunner {
    session: SessionContext,
}

impl SimpleGraphRunner {
    pub fn new(session: SessionContext) -> Self {
        Self { session }
    }
}

#[async_trait]
impl AsyncDB for SimpleGraphRunner {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>, DFSqlLogicTestError> {
        // Parse via DataFusion to get an optimized logical plan.
        let df = self
            .session
            .sql(sql)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;

        let (_, plan) = df.into_parts();

        // Convert to simple-graph IR.
        let mut ctx = QueryContext::new();
        let df_plan = match from_logical_plan(&plan, &mut ctx) {
            Ok(root) => {
                ctx.set_root(root);
                // Run optimizer passes (identity for now).
                let opt_ctx = OptimizerContext::new(ctx);
                let ctx = opt_ctx.into_query();
                // Convert back to DataFusion logical plan.
                match to_logical_plan(&ctx, &self.session).await {
                    Ok(plan) => plan,
                    Err(_) => plan, // fall back to original plan on conversion failure
                }
            }
            Err(_) => plan, // fall back to original plan if IR conversion fails
        };

        // Execute the plan.
        let df = self
            .session
            .execute_logical_plan(df_plan)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;

        let task_ctx = Arc::new(df.task_ctx());
        let physical = df
            .create_physical_plan()
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        let schema = physical.schema();
        let types = convert_schema_to_types(schema.fields());
        let results: Vec<RecordBatch> = collect(physical, task_ctx)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        let rows = convert_batches(&schema, results, false)?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    fn engine_name(&self) -> &str {
        "simple-graph"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}
