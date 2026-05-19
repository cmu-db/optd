//! [`OptdRunner`] — routes SQL through optd IR before execution.

use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use optd::{
    ExprSimplify, JoinOrdering, MarkJoinToSemiJoin, OperatorRewriteAdaptor, OptimizerContext,
    PassManager, PredicatePushdown, ProjectionElimination, QueryContext, QueryFormatConfig,
    SubqueryToJoin,
};
use sqllogictest::{AsyncDB, DBOutput};

use crate::config::OptdExtensionConfig;
use crate::explain_udfs::{
    ExplainStep, explain_steps_box, explain_steps_box_with_config, register_explain_udfs,
};
use crate::from_df::from_logical_plan;
use crate::to_df::to_logical_plan;

fn optimize(ctx: QueryContext) -> Result<QueryContext, optd::OptimizeError> {
    let mut opt = OptimizerContext::new(ctx);
    let mut pm = default_pass_manager();
    pm.run(&mut opt)?;
    if let Some(root) = opt.query.root() {
        let resolved = opt.rewrites.resolve(root);
        opt.query.set_root(resolved);
    }
    Ok(opt.into_query())
}

/// Returns the canonical optimizer pass pipeline used across all execution paths.
pub fn default_pass_manager() -> PassManager {
    let mut pm = PassManager::new();
    pm.add_pass(SubqueryToJoin);
    pm.add_pass(OperatorRewriteAdaptor::new(ExprSimplify));
    pm.add_pass(OperatorRewriteAdaptor::new(MarkJoinToSemiJoin));
    pm.add_pass(OperatorRewriteAdaptor::new(PredicatePushdown));
    pm.add_pass(OperatorRewriteAdaptor::new(ProjectionElimination));
    pm.add_pass(JoinOrdering::new());
    pm
}

pub struct OptdRunner {
    session: SessionContext,
}

#[derive(Debug)]
enum TryViaIrError {
    Unsupported(String),
    Failed(String),
}

impl std::fmt::Display for TryViaIrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(msg) | Self::Failed(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for TryViaIrError {}

pub enum RunnerOutput {
    StatementComplete,
    Rows {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
}

impl OptdRunner {
    pub fn new(session: SessionContext) -> Self {
        register_explain_udfs(&session);
        Self { session }
    }

    pub fn log_explain_steps_enabled(&self) -> bool {
        self.session
            .copied_config()
            .options()
            .extensions
            .get::<OptdExtensionConfig>()
            .map(|config| config.log_explain_steps)
            .unwrap_or_else(|| OptdExtensionConfig::default().log_explain_steps)
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<RunnerOutput, DFSqlLogicTestError> {
        let (schema, batches) = match self.try_via_ir(sql).await {
            Ok(pair) => pair,
            Err(TryViaIrError::Unsupported(_)) => {
                // IR conversion failed or unsupported — execute directly via DataFusion.
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
            Err(TryViaIrError::Failed(error)) => {
                return Err(DFSqlLogicTestError::DataFusion(
                    datafusion::error::DataFusionError::Execution(error),
                ));
            }
        };

        if batches.is_empty() && schema.fields().is_empty() {
            Ok(RunnerOutput::StatementComplete)
        } else {
            Ok(RunnerOutput::Rows { schema, batches })
        }
    }

    pub fn explain_steps_box(&self, sql: &str) -> Result<Vec<ExplainStep>, DFSqlLogicTestError> {
        explain_steps_box(sql, &self.session).map_err(DFSqlLogicTestError::DataFusion)
    }

    pub fn explain_steps_box_with_config(
        &self,
        sql: &str,
        config: QueryFormatConfig,
    ) -> Result<Vec<ExplainStep>, DFSqlLogicTestError> {
        explain_steps_box_with_config(sql, &self.session, config)
            .map_err(DFSqlLogicTestError::DataFusion)
    }
}

#[async_trait]
impl AsyncDB for OptdRunner {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>, DFSqlLogicTestError> {
        match self.execute_sql(sql).await? {
            RunnerOutput::StatementComplete => Ok(DBOutput::StatementComplete(0)),
            RunnerOutput::Rows { schema, batches } => {
                let types = convert_schema_to_types(schema.fields());
                let rows = convert_batches(&schema, batches, false)?;
                Ok(DBOutput::Rows { types, rows })
            }
        }
    }

    fn engine_name(&self) -> &str {
        "optd"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}

impl OptdRunner {
    /// Attempts SQL → optd IR → DataFusion plan round-trip.
    /// Returns an error if any step fails so the caller can fall back.
    async fn try_via_ir(
        &self,
        sql: &str,
    ) -> Result<(datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>), TryViaIrError> {
        // Parse without executing to avoid double-executing DDL.
        let plan = self
            .session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        // Only attempt IR conversion for relational query plans.
        match &plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_) => {}
            _ => return Err(TryViaIrError::Unsupported("non-query plan".into())),
        }

        reject_non_catalog_scans(&plan, &self.session).await?;

        let mut ctx = QueryContext::new();
        let root =
            from_logical_plan(&plan, &mut ctx).map_err(|e| TryViaIrError::Failed(e.to_string()))?;
        ctx.set_root(root);

        let ctx = optimize(ctx).map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        let df_plan = to_logical_plan(&ctx, &self.session)
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        // Optimize before execution so rules like COUNT(*) → scan work correctly.
        let optimized = self
            .session
            .state()
            .optimize(&df_plan)
            .map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        let df = self
            .session
            .execute_logical_plan(optimized)
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        let schema = df.schema().inner().clone();
        let batches = df
            .collect()
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))?;
        let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
        Ok((schema, batches))
    }
}

async fn reject_non_catalog_scans(
    plan: &LogicalPlan,
    session: &SessionContext,
) -> Result<(), TryViaIrError> {
    let mut scans = Vec::new();
    collect_table_scans(plan, &mut scans);
    for table in scans {
        if session.table_provider(table.clone()).await.is_err() {
            return Err(TryViaIrError::Unsupported(format!(
                "non-catalog table scan: {table}"
            )));
        }
    }
    Ok(())
}

fn collect_table_scans<'a>(
    plan: &'a LogicalPlan,
    out: &mut Vec<datafusion::common::TableReference>,
) {
    if let LogicalPlan::TableScan(scan) = plan {
        out.push(scan.table_name.clone());
    }
    for input in plan.inputs() {
        collect_table_scans(input, out);
    }
}

#[cfg(test)]
mod tests {
    use super::OptdRunner;
    use crate::config::OptdExtensionConfig;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[test]
    fn explain_step_logging_follows_session_extension_value() {
        let session = SessionContext::new_with_config(SessionConfig::new().with_option_extension(
            OptdExtensionConfig {
                log_explain_steps: false,
            },
        ));
        let runner = OptdRunner::new(session);
        assert!(!runner.log_explain_steps_enabled());
    }

    #[test]
    fn explain_step_logging_uses_extension_default_when_missing() {
        let runner = OptdRunner::new(SessionContext::new());
        assert!(runner.log_explain_steps_enabled());
    }
}
