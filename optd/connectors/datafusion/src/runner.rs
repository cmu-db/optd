//! [`OptdRunner`] — routes SQL through optd IR before execution.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_optimizer::Analyzer;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use optd_core::{
    ExprSimplify, HolisticUnnesting, JoinOrdering, JoinTreeNormalize, MarkJoinToSemiJoin,
    OperatorRewriteAdaptor, OptimizerContext, PassManager, PredicatePushdown,
    ProjectionElimination, QueryContext, QueryFormatConfig, SubqueryToJoin,
};
use sqllogictest::{AsyncDB, DBOutput};

use crate::config::OptdExtensionConfig;
use crate::explain_udfs::{
    ExplainStep, explain_steps_box, explain_steps_box_with_config, register_explain_udfs,
};
use crate::from_df_logical::from_logical_plan;
use crate::to_df_logical::to_logical_plan;

fn optimize(ctx: QueryContext) -> Result<QueryContext, optd_core::OptimizeError> {
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
    pm.add_pass(HolisticUnnesting);
    pm.add_pass(OperatorRewriteAdaptor::new(MarkJoinToSemiJoin));
    pm.add_pass(OperatorRewriteAdaptor::new(PredicatePushdown));
    pm.add_pass(JoinTreeNormalize::new());
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

    pub fn optd_enabled(&self) -> bool {
        self.session
            .copied_config()
            .options()
            .extensions
            .get::<OptdExtensionConfig>()
            .map(|config| config.optd_enabled)
            .unwrap_or_else(|| OptdExtensionConfig::default().optd_enabled)
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
        let (schema, batches) = if self.optd_enabled() {
            match self.try_via_ir(sql).await {
                Ok(pair) => pair,
                Err(TryViaIrError::Unsupported(_)) => {
                    // IR conversion failed or unsupported — execute directly via DataFusion.
                    self.execute_datafusion(sql).await?
                }
                Err(TryViaIrError::Failed(error)) => {
                    return Err(DFSqlLogicTestError::DataFusion(
                        datafusion::error::DataFusionError::Execution(error),
                    ));
                }
            }
        } else {
            self.execute_datafusion(sql).await?
        };

        if batches.is_empty() && schema.fields().is_empty() {
            Ok(RunnerOutput::StatementComplete)
        } else {
            Ok(RunnerOutput::Rows { schema, batches })
        }
    }

    async fn execute_datafusion(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), DFSqlLogicTestError> {
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
        let schema = df.schema().inner().clone();
        let batches = df
            .collect()
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
        Ok((schema, batches))
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
    /// Attempts SQL → optd IR → DataFusion logical plan.
    /// Direct physical planning is intentionally inactive until unnesting can
    /// decorrelate the TPC-H subquery shapes that require it.
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

        let plan = self.logical_plan_via_ir(&plan).await?;
        let df = self
            .session
            .execute_logical_plan(plan)
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

    async fn logical_plan_via_ir(&self, plan: &LogicalPlan) -> Result<LogicalPlan, TryViaIrError> {
        if !is_supported_relational_plan(plan) {
            return Err(TryViaIrError::Unsupported("non-query plan".into()));
        }

        let plan =
            apply_type_coercion(plan.clone()).map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        reject_non_catalog_scans(&plan, &self.session).await?;

        let mut ctx = QueryContext::new();
        let root =
            from_logical_plan(&plan, &mut ctx).map_err(|e| TryViaIrError::Failed(e.to_string()))?;
        ctx.set_root(root);

        let ctx = optimize(ctx).map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        to_logical_plan(&ctx, &self.session)
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))
    }
}

fn apply_type_coercion(plan: LogicalPlan) -> datafusion::error::Result<LogicalPlan> {
    let options = datafusion_common::config::ConfigOptions::default();
    Analyzer::with_rules(vec![Arc::new(TypeCoercion::new())]).execute_and_check(
        plan,
        &options,
        |_, _| {},
    )
}

fn is_supported_relational_plan(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
    )
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

fn collect_table_scans(plan: &LogicalPlan, out: &mut Vec<datafusion::common::TableReference>) {
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
    use crate::setup::session_context_with_information_schema;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[test]
    fn explain_step_logging_follows_session_extension_value() {
        let session = SessionContext::new_with_config(SessionConfig::new().with_option_extension(
            OptdExtensionConfig {
                optd_enabled: true,
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

    #[test]
    fn optd_enabled_follows_session_extension_value() {
        let session = SessionContext::new_with_config(SessionConfig::new().with_option_extension(
            OptdExtensionConfig {
                optd_enabled: false,
                log_explain_steps: true,
            },
        ));
        let runner = OptdRunner::new(session);
        assert!(!runner.optd_enabled());
    }

    #[test]
    fn optd_enabled_uses_extension_default_when_missing() {
        let runner = OptdRunner::new(SessionContext::new());
        assert!(runner.optd_enabled());
    }

    #[test]
    fn default_pass_manager_runs_holistic_unnesting_after_expr_simplify() {
        let mut opt = optd_core::OptimizerContext::new(optd_core::QueryContext::new());
        let mut pm = super::default_pass_manager();
        pm.run(&mut opt).unwrap();

        let passes: Vec<_> = pm.profiles().iter().map(|profile| profile.pass).collect();
        let expr_simplify = passes
            .iter()
            .position(|pass| *pass == "ExprSimplify")
            .unwrap();
        let unnesting = passes
            .iter()
            .position(|pass| *pass == "HolisticUnnesting")
            .unwrap();

        assert_eq!(unnesting, expr_simplify + 1);
    }

    #[tokio::test]
    async fn show_tables_falls_back_to_datafusion() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        let result = runner.execute_sql("SHOW TABLES").await.unwrap();
        match result {
            super::RunnerOutput::Rows { batches, .. } => {
                assert!(!batches.is_empty());
            }
            super::RunnerOutput::StatementComplete => {
                panic!("SHOW TABLES should return rows");
            }
        }
    }

    #[tokio::test]
    async fn explain_is_not_an_ir_query() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        let result = runner
            .try_via_ir("EXPLAIN SELECT * FROM (VALUES (1)) AS t(a)")
            .await;

        assert!(matches!(result, Err(super::TryViaIrError::Unsupported(_))));
    }

    #[tokio::test]
    async fn explain_analyze_is_not_an_ir_query() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        let result = runner
            .try_via_ir("EXPLAIN ANALYZE SELECT * FROM (VALUES (1)) AS t(a)")
            .await;

        assert!(matches!(result, Err(super::TryViaIrError::Unsupported(_))));
    }

    #[tokio::test]
    async fn optd_ir_execution_uses_logical_converter_while_physical_is_inactive() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        let (_, batches) = runner
            .try_via_ir(
                "SELECT a \
                 FROM (VALUES (1), (2)) AS t(a) \
                 WHERE a = ( \
                     SELECT max(b) \
                     FROM (VALUES (1), (2)) AS u(b) \
                     WHERE b = t.a \
                 )",
            )
            .await
            .unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }

    #[tokio::test]
    async fn optd_ir_execution_preserves_empty_query_schema() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        let result = runner
            .execute_sql("SELECT a FROM (VALUES (1)) AS t(a) WHERE a > 1")
            .await
            .unwrap();

        match result {
            super::RunnerOutput::Rows { schema, batches } => {
                assert_eq!(schema.fields().len(), 1);
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    0
                );
            }
            super::RunnerOutput::StatementComplete => {
                panic!("empty SELECT should preserve its row schema");
            }
        }
    }

    #[test]
    fn explain_steps_unwraps_explain_analyze() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        let steps = runner
            .explain_steps_box("EXPLAIN ANALYZE SELECT * FROM (VALUES (1)) AS t(a)")
            .unwrap();

        assert!(!steps.is_empty());
    }
}
