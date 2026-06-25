//! [`OptdRunner`] — routes SQL through optd IR before execution.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion_optimizer::Analyzer;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use optd_core::{
    Catalog, ExprSimplify, HolisticUnnesting, JoinOrdering, JoinTreeNormalize, MarkJoinToSemiJoin,
    OperatorRewriteAdaptor, OptimizerContext, PassManager, PredicatePushdown,
    ProjectionElimination, QueryContext, QueryFormatConfig, SubqueryToJoin,
};
use sqllogictest::{AsyncDB, DBOutput};

use crate::config::OptdExtensionConfig;
use crate::explain_udfs::{
    ExplainStep, explain_steps_box, explain_steps_box_with_config, register_explain_udfs,
};
use crate::from_df_logical::from_logical_plan;
use crate::runtime_statistics::RuntimeStatisticsCatalogBuilder;
use crate::to_df_logical::to_logical_plan;
use crate::to_df_physical::{ToPhysicalError, to_physical_plan};

fn optimize(
    ctx: QueryContext,
    catalog: Option<Arc<dyn Catalog>>,
) -> Result<QueryContext, optd_core::OptimizeError> {
    let mut opt = match catalog {
        Some(catalog) => OptimizerContext::with_catalog(ctx, catalog),
        None => OptimizerContext::new(ctx),
    };
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
    runtime_stats: RuntimeStatisticsCatalogBuilder,
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum IrExecutionPath {
    Logical,
    Physical,
    PhysicalUnsupported(String),
}

impl OptdRunner {
    pub fn new(session: SessionContext) -> Self {
        register_explain_udfs(&session);
        let runtime_stats = RuntimeStatisticsCatalogBuilder::new(session.clone());
        Self {
            session,
            runtime_stats,
        }
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

    pub fn physical_planning_enabled(&self) -> bool {
        self.session
            .copied_config()
            .options()
            .extensions
            .get::<OptdExtensionConfig>()
            .map(|config| config.physical_planning)
            .unwrap_or_else(|| OptdExtensionConfig::default().physical_planning)
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

    pub async fn explain_direct_physical_plan(
        &self,
        sql: &str,
    ) -> Result<String, DFSqlLogicTestError> {
        let plan = self.parse_logical_plan(sql).await.map_err(|err| {
            DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Plan(
                err.to_string(),
            ))
        })?;
        let plan = self
            .optimized_ir_from_logical_plan(&plan)
            .await
            .map_err(|err| {
                DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Plan(
                    err.to_string(),
                ))
            })?;
        let physical = to_physical_plan(&plan, &self.session)
            .await
            .map_err(|err| {
                DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Plan(
                    err.to_string(),
                ))
            })?;
        Ok(displayable(physical.as_ref()).indent(true).to_string())
    }

    pub async fn explain_logical_physical_plan(
        &self,
        sql: &str,
    ) -> Result<String, DFSqlLogicTestError> {
        let plan = self.parse_logical_plan(sql).await.map_err(|err| {
            DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Plan(
                err.to_string(),
            ))
        })?;
        let ctx = self
            .optimized_ir_from_logical_plan(&plan)
            .await
            .map_err(|err| {
                DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Plan(
                    err.to_string(),
                ))
            })?;
        let plan = to_logical_plan(&ctx, &self.session).await.map_err(|err| {
            DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Plan(
                err.to_string(),
            ))
        })?;
        let physical = self
            .session
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        Ok(displayable(physical.as_ref()).indent(true).to_string())
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
    /// Attempts SQL → optd IR → DataFusion execution.
    async fn try_via_ir(
        &self,
        sql: &str,
    ) -> Result<(datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>), TryViaIrError> {
        self.try_via_ir_with_path(sql)
            .await
            .map(|(schema, batches, _)| (schema, batches))
    }

    async fn try_via_ir_with_path(
        &self,
        sql: &str,
    ) -> Result<
        (
            datafusion::arrow::datatypes::SchemaRef,
            Vec<RecordBatch>,
            IrExecutionPath,
        ),
        TryViaIrError,
    > {
        // Parse without executing to avoid double-executing DDL.
        let plan = self.parse_logical_plan(sql).await?;
        let ctx = self.optimized_ir_from_logical_plan(&plan).await?;
        self.execute_optimized_ir(&ctx).await
    }

    async fn parse_logical_plan(&self, sql: &str) -> Result<LogicalPlan, TryViaIrError> {
        self.session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))
    }

    async fn optimized_ir_from_logical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<QueryContext, TryViaIrError> {
        if !is_supported_relational_plan(plan) {
            return Err(TryViaIrError::Unsupported("non-query plan".into()));
        }

        let plan =
            apply_type_coercion(plan.clone()).map_err(|e| TryViaIrError::Failed(e.to_string()))?;

        reject_non_catalog_scans(&plan, &self.session).await?;

        let catalog = self
            .runtime_stats
            .build_for_plan(&plan)
            .await
            .map_err(TryViaIrError::Failed)?;

        let mut ctx = QueryContext::new();
        let root =
            from_logical_plan(&plan, &mut ctx).map_err(|e| TryViaIrError::Failed(e.to_string()))?;
        ctx.set_root(root);

        optimize(ctx, Some(catalog)).map_err(|e| TryViaIrError::Failed(e.to_string()))
    }

    async fn execute_optimized_ir(
        &self,
        ctx: &QueryContext,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, IrExecutionPath), TryViaIrError> {
        if self.physical_planning_enabled() {
            match self.execute_physical_ir(ctx).await {
                Ok((schema, batches)) => {
                    return Ok((schema, batches, IrExecutionPath::Physical));
                }
                Err(ToPhysicalError::Unsupported(msg)) => {
                    // Unsupported direct physical shapes use the stable
                    // logical-conversion path. Build/execution errors are
                    // converter bugs and intentionally do not fall back.
                    let (schema, batches) = self.execute_logical_ir(ctx).await?;
                    return Ok((schema, batches, IrExecutionPath::PhysicalUnsupported(msg)));
                }
                Err(err) => return Err(TryViaIrError::Failed(err.to_string())),
            }
        }

        let (schema, batches) = self.execute_logical_ir(ctx).await?;
        Ok((schema, batches, IrExecutionPath::Logical))
    }

    async fn execute_logical_ir(
        &self,
        ctx: &QueryContext,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), TryViaIrError> {
        let plan = to_logical_plan(ctx, &self.session)
            .await
            .map_err(|e| TryViaIrError::Failed(e.to_string()))?;
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

    async fn execute_physical_ir(
        &self,
        ctx: &QueryContext,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), ToPhysicalError> {
        let plan = to_physical_plan(ctx, &self.session).await?;
        let schema = plan.schema();
        let batches = collect(plan, self.session.state().task_ctx())
            .await
            .map_err(ToPhysicalError::Build)?;
        let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
        Ok((schema, batches))
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
        if is_information_schema_table(&table) {
            return Err(TryViaIrError::Unsupported(format!(
                "information-schema table scan: {table}"
            )));
        }
        if session.table_provider(table.clone()).await.is_err() {
            return Err(TryViaIrError::Unsupported(format!(
                "non-catalog table scan: {table}"
            )));
        }
    }
    Ok(())
}

fn is_information_schema_table(table: &TableReference) -> bool {
    table.schema() == Some("information_schema")
        || table.to_string().starts_with("information_schema.")
        || table.to_string().contains(".information_schema.")
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
    use super::{IrExecutionPath, OptdRunner};
    use crate::config::OptdExtensionConfig;
    use crate::setup::session_context_with_information_schema;
    use datafusion::arrow::array::Int64Array;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn session_with_optd_config(config: OptdExtensionConfig) -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_option_extension(config))
    }

    fn physical_runner() -> OptdRunner {
        OptdRunner::new(session_with_optd_config(OptdExtensionConfig {
            optd_enabled: true,
            log_explain_steps: true,
            physical_planning: true,
        }))
    }

    #[test]
    fn explain_step_logging_follows_session_extension_value() {
        let session = session_with_optd_config(OptdExtensionConfig {
            optd_enabled: true,
            log_explain_steps: false,
            physical_planning: false,
        });
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
        let session = session_with_optd_config(OptdExtensionConfig {
            optd_enabled: false,
            log_explain_steps: true,
            physical_planning: false,
        });
        let runner = OptdRunner::new(session);
        assert!(!runner.optd_enabled());
    }

    #[test]
    fn optd_enabled_uses_extension_default_when_missing() {
        let runner = OptdRunner::new(SessionContext::new());
        assert!(runner.optd_enabled());
    }

    #[test]
    fn physical_planning_follows_session_extension_value() {
        let runner = physical_runner();
        assert!(runner.physical_planning_enabled());
    }

    #[test]
    fn physical_planning_uses_extension_default_when_missing() {
        let runner = OptdRunner::new(SessionContext::new());
        assert!(runner.physical_planning_enabled());
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
    async fn set_optd_physical_planning_disables_physical_path() {
        let runner = OptdRunner::new(session_context_with_information_schema());
        assert!(runner.physical_planning_enabled());

        runner
            .execute_sql("SET optd.physical_planning = false")
            .await
            .unwrap();

        assert!(!runner.physical_planning_enabled());
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
    async fn optd_ir_execution_uses_logical_converter_when_physical_is_disabled() {
        let runner = OptdRunner::new(session_with_optd_config(OptdExtensionConfig {
            optd_enabled: true,
            log_explain_steps: true,
            physical_planning: false,
        }));
        let (_, batches, path) = runner
            .try_via_ir_with_path(
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

        assert_eq!(path, IrExecutionPath::Logical);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }

    #[tokio::test]
    async fn physical_path_executes_values_filter_projection() {
        let runner = physical_runner();
        let (_, batches, path) = runner
            .try_via_ir_with_path(
                "SELECT a + 10 AS v FROM (VALUES (1), (2), (3)) AS t(a) WHERE a > 1",
            )
            .await
            .unwrap();

        assert_eq!(path, IrExecutionPath::Physical);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[12, 13]);
    }

    #[tokio::test]
    async fn physical_path_executes_aggregate() {
        let runner = physical_runner();
        let (_, batches, path) = runner
            .try_via_ir_with_path("SELECT sum(a) FROM (VALUES (1), (2), (3)) AS t(a)")
            .await
            .unwrap();

        assert_eq!(path, IrExecutionPath::Physical);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn physical_path_executes_join() {
        let runner = physical_runner();
        let (_, batches, path) = runner
            .try_via_ir_with_path(
                "SELECT a FROM (VALUES (1), (2)) AS t(a) \
                 JOIN (VALUES (2), (3)) AS u(b) ON a = b",
            )
            .await
            .unwrap();

        assert_eq!(path, IrExecutionPath::Physical);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
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

    #[tokio::test]
    async fn physical_path_preserves_empty_query_schema() {
        let runner = physical_runner();
        let (schema, batches, path) = runner
            .try_via_ir_with_path("SELECT a FROM (VALUES (1)) AS t(a) WHERE a > 1")
            .await
            .unwrap();

        assert_eq!(path, IrExecutionPath::Physical);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            0
        );
    }

    #[tokio::test]
    async fn physical_path_executes_projected_in_left_mark_join() {
        let runner = physical_runner();
        let (_, batches, path) = runner
            .try_via_ir_with_path(
                "SELECT a IN (SELECT b FROM (VALUES (2), (CAST(NULL AS BIGINT))) AS u(b)) \
                 FROM (VALUES (1)) AS t(a)",
            )
            .await
            .unwrap();

        assert_eq!(path, IrExecutionPath::Physical);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn direct_physical_plan_extracts_common_or_join_key() {
        let runner = physical_runner();
        let plan = runner
            .explain_direct_physical_plan(
                "SELECT sum(a.v) \
                 FROM (VALUES (1, 10), (2, 20)) AS a(k, v), \
                      (VALUES (1, 'x'), (2, 'y')) AS b(k, tag) \
                 WHERE (a.k = b.k AND a.v > 5 AND b.tag = 'x') \
                    OR (a.k = b.k AND a.v > 15 AND b.tag = 'y')",
            )
            .await
            .unwrap();

        assert!(plan.contains("HashJoinExec"), "{plan}");
        assert!(!plan.contains("NestedLoopJoinExec"), "{plan}");
    }

    #[tokio::test]
    async fn direct_physical_plan_uses_hash_join_for_semi_anti_residuals() {
        let runner = physical_runner();
        let plan = runner
            .explain_direct_physical_plan(
                "SELECT a.k \
                 FROM (VALUES (1, 10), (2, 20)) AS a(k, s) \
                 WHERE EXISTS ( \
                     SELECT * FROM (VALUES (1, 30), (2, 20)) AS b(k, s) \
                     WHERE b.k = a.k AND b.s != a.s \
                 ) \
                 AND NOT EXISTS ( \
                     SELECT * FROM (VALUES (1, 30), (2, 20)) AS c(k, s) \
                     WHERE c.k = a.k AND c.s != a.s \
                 )",
            )
            .await
            .unwrap();

        assert!(plan.contains("HashJoinExec"), "{plan}");
        assert!(plan.contains("join_type=LeftAnti"), "{plan}");
        assert!(!plan.contains("NestedLoopJoinExec"), "{plan}");
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
