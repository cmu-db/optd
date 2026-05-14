//! [`SimpleGraphRunner`] — routes SQL through simple-graph IR before execution.

use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use simple_graph::{OptimizerContext, QueryContext};
use sqllogictest::{AsyncDB, DBOutput};

use crate::from_df::from_logical_plan;
use crate::to_df::to_logical_plan;

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
        let (schema, results) = match self.try_via_ir(sql).await {
            Ok(pair) => pair,
            Err(e) if e == "non-query plan" => {
                // DDL and other non-query statements — execute directly.
                let plan = self
                    .session
                    .state()
                    .create_logical_plan(sql)
                    .await
                    .map_err(DFSqlLogicTestError::DataFusion)?;
                let df = self
                    .session
                    .execute_logical_plan(plan)
                    .await
                    .map_err(DFSqlLogicTestError::DataFusion)?;
                let batches = df
                    .collect()
                    .await
                    .map_err(DFSqlLogicTestError::DataFusion)?;
                let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| {
                    std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty())
                });
                (schema, batches)
            }
            Err(e) => {
                return Err(DFSqlLogicTestError::DataFusion(
                    datafusion::error::DataFusionError::Plan(e),
                ));
            }
        };

        let types = convert_schema_to_types(schema.fields());
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

impl SimpleGraphRunner {
    /// Attempts SQL → simple-graph IR → DataFusion plan round-trip.
    /// Returns an error if any step fails so the caller can fall back.
    async fn try_via_ir(
        &self,
        sql: &str,
    ) -> Result<(datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>), String> {
        // Parse without executing to avoid double-executing DDL.
        let plan = self
            .session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| e.to_string())?;

        // Only attempt IR conversion for relational query plans.
        match &plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::SubqueryAlias(_) => {}
            _ => return Err("non-query plan".into()),
        }

        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).map_err(|e| e.to_string())?;
        ctx.set_root(root);

        let opt_ctx = OptimizerContext::new(ctx);
        let ctx = opt_ctx.into_query();

        let df_plan = to_logical_plan(&ctx, &self.session)
            .await
            .map_err(|e| e.to_string())?;

        // Optimize before execution so rules like COUNT(*) → scan work correctly.
        let optimized = self
            .session
            .state()
            .optimize(&df_plan)
            .map_err(|e| e.to_string())?;

        let df = self
            .session
            .execute_logical_plan(optimized)
            .await
            .map_err(|e| e.to_string())?;

        let schema = df.schema().inner().clone();
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
        Ok((schema, batches))
    }
}
