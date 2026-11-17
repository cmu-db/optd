//! [`OptdRunner`] — routes SQL through optd IR before execution.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::SessionContext;
use datafusion_optimizer::Analyzer;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use optd_catalog::query_history::QueryStatus;
use optd_core::{
    Catalog, ExprSimplify, HolisticUnnesting, JoinOrdering, JoinTreeNormalize, MarkJoinToSemiJoin,
    OperatorRewriteAdaptor, OptimizerContext, PassManager, PlannedQuery, PredicatePushdown,
    ProjectionElimination, QueryContext, QueryFormatConfig, SubqueryToJoin,
};
use sea_orm::DatabaseConnection;
use sqllogictest::{AsyncDB, DBOutput};

use crate::config::OptdExtensionConfig;
use crate::explain_udfs::{
    ExplainStep, explain_steps_box, explain_steps_box_with_config, register_explain_udfs,
};
use crate::from_df_logical::from_logical_plan;
use crate::observed_execution::{StatementObservation, execute_observed_sql};
use crate::query_history::{QueryHistory, QueryHistoryOptions, QueryHistoryReport, RequestHistory};
use crate::runtime_statistics::RuntimeStatisticsCatalogBuilder;
use crate::statistics_udfs::build_persistent_statistics_with_snapshot;
use crate::to_df_logical::to_logical_plan;
use crate::to_df_physical::{ToPhysicalError, to_physical_plan};

fn optimize(mut opt: OptimizerContext) -> Result<PlannedQuery, optd_core::OptimizeError> {
    let mut pm = default_pass_manager();
    pm.run(&mut opt)?;
    if let Some(root) = opt.query.root() {
        let resolved = opt.rewrites.resolve(root);
        opt.query.set_root(resolved);
    }
    Ok(opt.into_planned_query())
}

pub(crate) async fn optimizer_context_from_logical_plan(
    session: &SessionContext,
    runtime_stats: &RuntimeStatisticsCatalogBuilder,
    plan: &LogicalPlan,
) -> Result<OptimizerContext, TryViaIrError> {
    if !is_supported_relational_plan(plan) {
        return Err(TryViaIrError::Unsupported("non-query plan".into()));
    }

    let plan =
        apply_type_coercion(plan.clone()).map_err(|e| TryViaIrError::Failed(e.to_string()))?;

    reject_non_catalog_scans(&plan, session).await?;

    let catalog = runtime_stats
        .build_for_plan(&plan)
        .await
        .map_err(TryViaIrError::Failed)?;

    optimizer_context_from_prepared_plan(&plan, catalog)
}

pub(crate) async fn optimizer_context_from_logical_plan_with_catalog(
    session: &SessionContext,
    plan: &LogicalPlan,
    catalog: Arc<dyn Catalog>,
) -> Result<OptimizerContext, TryViaIrError> {
    if !is_supported_relational_plan(plan) {
        return Err(TryViaIrError::Unsupported("non-query plan".into()));
    }

    let plan =
        apply_type_coercion(plan.clone()).map_err(|e| TryViaIrError::Failed(e.to_string()))?;

    reject_non_catalog_scans(&plan, session).await?;

    optimizer_context_from_prepared_plan(&plan, catalog)
}

fn optimizer_context_from_prepared_plan(
    plan: &LogicalPlan,
    catalog: Arc<dyn Catalog>,
) -> Result<OptimizerContext, TryViaIrError> {
    let mut ctx = QueryContext::new();
    let root =
        from_logical_plan(plan, &mut ctx).map_err(|e| TryViaIrError::Failed(e.to_string()))?;
    ctx.set_root(root);

    Ok(OptimizerContext::new(ctx, catalog))
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
    statistics_source: StatisticsSource,
    last_observation: Mutex<Option<StatementObservation>>,
    query_history: Option<QueryHistory>,
}

/// Statistics source used while constructing query-local planner catalogs.
enum StatisticsSource {
    /// Collect and cache statistics directly from DataFusion providers.
    Runtime(RuntimeStatisticsCatalogBuilder),
    /// Read statistics from the snapshot-aware persistent catalog.
    Persistent {
        db: DatabaseConnection,
        catalog_name: String,
    },
}

#[derive(Debug)]
pub(crate) enum TryViaIrError {
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

/// Result and independent, per-call history acknowledgments.
pub struct RunnerExecution {
    pub result: Result<RunnerOutput, DFSqlLogicTestError>,
    pub history: Option<QueryHistoryReport>,
}

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
            statistics_source: StatisticsSource::Runtime(runtime_stats),
            last_observation: Mutex::new(None),
            query_history: None,
        }
    }

    /// Creates a runner that reads statistics for `catalog_name` from `catalog_db`.
    ///
    /// Missing or invalidated payloads and tables in other DataFusion catalogs use optd's normal
    /// fallback estimates; this mode does not scan tables automatically during optimization.
    pub fn new_with_persistent_catalog(
        session: SessionContext,
        catalog_name: impl Into<String>,
        catalog_db: DatabaseConnection,
    ) -> Self {
        register_explain_udfs(&session);
        Self {
            session,
            statistics_source: StatisticsSource::Persistent {
                db: catalog_db,
                catalog_name: catalog_name.into(),
            },
            last_observation: Mutex::new(None),
            query_history: None,
        }
    }

    /// Enables SQL request history in this runner's persistent metadata database.
    /// Runtime-only runners are rejected: snapshot references must share that database.
    pub fn with_query_history(
        mut self,
        options: QueryHistoryOptions,
    ) -> datafusion::error::Result<Self> {
        let StatisticsSource::Persistent { db, .. } = &self.statistics_source else {
            return Err(datafusion::error::DataFusionError::Plan(
                "query history requires a persistent catalog".into(),
            ));
        };
        self.query_history = Some(QueryHistory::new(db.clone(), options));
        Ok(self)
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

    /// Last fallback execution report. Source errors and metadata failures are separate;
    /// the shared executor API is preferable for concurrent callers needing per-call reports.
    pub fn last_observation(&self) -> Option<StatementObservation> {
        self.last_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<RunnerOutput, DFSqlLogicTestError> {
        self.execute_sql_with_history(sql).await.result
    }

    /// Executes unchanged input and reports history failures separately, including
    /// when the query itself fails. Dropped futures may leave unfinished instances.
    pub async fn execute_sql_with_history(&self, sql: &str) -> RunnerExecution {
        *self
            .last_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let state = self.session.state();
        let config = state
            .config_options()
            .extensions
            .get::<OptdExtensionConfig>()
            .cloned()
            .unwrap_or_default();
        let mut history = match &self.query_history {
            Some(store) => {
                let statistics_catalog = match &self.statistics_source {
                    StatisticsSource::Persistent { catalog_name, .. } => Some(catalog_name),
                    StatisticsSource::Runtime(_) => None,
                };
                let configuration = serde_json::json!({
                    "statistics_catalog": statistics_catalog,
                    "optd_enabled": config.optd_enabled,
                    "physical_planning": config.physical_planning,
                    "log_explain_steps": config.log_explain_steps,
                    "target_partitions": state.config().target_partitions(),
                });
                Some(store.begin(&state, sql, configuration).await)
            }
            None => None,
        };
        let result = self
            .execute_request(sql, &state, &config, history.as_mut())
            .await;
        RunnerExecution {
            result,
            history: history.map(RequestHistory::into_report),
        }
    }

    async fn execute_request(
        &self,
        sql: &str,
        state: &SessionState,
        config: &OptdExtensionConfig,
        mut history: Option<&mut RequestHistory<'_>>,
    ) -> Result<RunnerOutput, DFSqlLogicTestError> {
        let failed = |error: TryViaIrError| {
            DFSqlLogicTestError::DataFusion(datafusion::error::DataFusionError::Execution(
                error.to_string(),
            ))
        };
        let (schema, batches) = if config.optd_enabled {
            let planning = async {
                let plan = state
                    .create_logical_plan(sql)
                    .await
                    .map_err(|error| TryViaIrError::Failed(error.to_string()))?;
                self.optimized_ir_with_history(&plan, history.as_deref_mut())
                    .await
            }
            .await;
            match planning {
                Ok(plan) => {
                    if let Some(history) = history.as_deref_mut() {
                        history
                            .finish(QueryStatus::Optimized, "optd_optimized", None)
                            .await;
                    }
                    let (schema, batches, _) = self
                        .execute_optimized_ir_with_mode(&plan, config.physical_planning)
                        .await
                        .map_err(failed)?;
                    (schema, batches)
                }
                Err(TryViaIrError::Unsupported(reason)) => {
                    if let Some(history) = history.as_deref_mut() {
                        history
                            .finish(QueryStatus::Bypassed, "unsupported_by_optd", Some(&reason))
                            .await;
                    }
                    self.execute_datafusion(sql).await?
                }
                Err(error) => {
                    if let Some(history) = history.as_deref_mut() {
                        history
                            .finish(
                                QueryStatus::Failed,
                                "planning_failed",
                                Some(&error.to_string()),
                            )
                            .await;
                    }
                    return Err(failed(error));
                }
            }
        } else {
            if let Some(history) = history {
                history
                    .finish(QueryStatus::Bypassed, "optd_disabled", None)
                    .await;
            }
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
        let execution = execute_observed_sql(&self.session, sql)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        if execution.observation.has_metadata_failure() {
            eprintln!(
                "optd metadata observation requires attention; source effects are not rolled back and SQL must not be replayed: {:?}",
                execution.observation
            );
        }
        *self
            .last_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(execution.observation);
        let output = execution.result.map_err(DFSqlLogicTestError::DataFusion)?;
        Ok((output.schema, output.batches))
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
    #[cfg(test)]
    async fn try_via_ir(
        &self,
        sql: &str,
    ) -> Result<(datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>), TryViaIrError> {
        self.try_via_ir_with_path(sql)
            .await
            .map(|(schema, batches, _)| (schema, batches))
    }

    #[cfg(test)]
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
    ) -> Result<PlannedQuery, TryViaIrError> {
        self.optimized_ir_with_history(plan, None).await
    }

    async fn optimized_ir_with_history(
        &self,
        plan: &LogicalPlan,
        history: Option<&mut RequestHistory<'_>>,
    ) -> Result<PlannedQuery, TryViaIrError> {
        // Writes/DDL must reach the shared executor even if cached statistics cannot
        // be read; do not attempt optimizer metadata construction for non-query plans.
        if !is_supported_relational_plan(plan) {
            return Err(TryViaIrError::Unsupported("non-query plan".into()));
        }
        let opt = match &self.statistics_source {
            StatisticsSource::Runtime(runtime_stats) => {
                optimizer_context_from_logical_plan(&self.session, runtime_stats, plan).await?
            }
            StatisticsSource::Persistent { db, catalog_name } => {
                let catalog = build_persistent_statistics_with_snapshot(
                    &self.session,
                    db.clone(),
                    catalog_name,
                    plan,
                )
                .await
                .map_err(|error| TryViaIrError::Failed(error.to_string()))?;
                if let Some(history) = history {
                    history.attach_snapshot(catalog.snapshot_id).await;
                }
                optimizer_context_from_logical_plan_with_catalog(
                    &self.session,
                    plan,
                    catalog.catalog,
                )
                .await?
            }
        };
        optimize(opt).map_err(|e| TryViaIrError::Failed(e.to_string()))
    }

    #[cfg(test)]
    async fn execute_optimized_ir(
        &self,
        opt: &PlannedQuery,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, IrExecutionPath), TryViaIrError> {
        self.execute_optimized_ir_with_mode(opt, self.physical_planning_enabled())
            .await
    }

    async fn execute_optimized_ir_with_mode(
        &self,
        opt: &PlannedQuery,
        physical_planning: bool,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, IrExecutionPath), TryViaIrError> {
        if physical_planning {
            match self.execute_physical_ir(opt).await {
                Ok((schema, batches)) => {
                    return Ok((schema, batches, IrExecutionPath::Physical));
                }
                Err(ToPhysicalError::Unsupported(msg)) => {
                    // Unsupported direct physical shapes use the stable
                    // logical-conversion path. Build/execution errors are
                    // converter bugs and intentionally do not fall back.
                    let (schema, batches) = self.execute_logical_ir(opt).await?;
                    return Ok((schema, batches, IrExecutionPath::PhysicalUnsupported(msg)));
                }
                Err(err) => return Err(TryViaIrError::Failed(err.to_string())),
            }
        }

        let (schema, batches) = self.execute_logical_ir(opt).await?;
        Ok((schema, batches, IrExecutionPath::Logical))
    }

    async fn execute_logical_ir(
        &self,
        planned: &PlannedQuery,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), TryViaIrError> {
        let plan = to_logical_plan(planned, &self.session)
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
        planned: &PlannedQuery,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), ToPhysicalError> {
        let plan = to_physical_plan(planned, &self.session).await?;
        let state = self.session.state();
        let plan = DefaultPhysicalPlanner::default()
            .optimize_physical_plan(plan, &state, |_, _| {})
            .map_err(ToPhysicalError::Build)?;
        let schema = plan.schema();
        let batches = collect(plan, state.task_ctx())
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
    use crate::persistent_catalog::install_persistent_catalog;
    use crate::setup::session_context_with_information_schema;
    use crate::test_utils::test_sqlite_connection;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use optd_catalog::Catalog;
    use optd_core::CardinalityEstimationV1;
    use std::sync::Arc;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_runner_uses_catalog_statistics_without_runtime_collection() {
        let (_temp_dir, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql(
                "CREATE TABLE numbers AS \
                 SELECT * FROM (VALUES (1), (2), (2)) AS source(value)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("SELECT collect_stats('numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        for sql in [
            "CREATE DATABASE other",
            "CREATE SCHEMA other.public",
            "CREATE TABLE other.public.numbers AS SELECT 1 AS value",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let runner = OptdRunner::new_with_persistent_catalog(session, "datafusion", db.clone());
        let plan = runner
            .parse_logical_plan("SELECT * FROM numbers")
            .await
            .unwrap();
        let planned = runner.optimized_ir_from_logical_plan(&plan).await.unwrap();
        let root = planned.query.root().unwrap();
        let mut analyses = planned.analyze();
        let estimate = analyses
            .get::<CardinalityEstimationV1>(&planned.query, root)
            .unwrap();
        assert_eq!(estimate.rows.value, 3.0);

        let foreign_plan = runner
            .parse_logical_plan("SELECT * FROM other.public.numbers")
            .await
            .unwrap();
        let foreign_planned = runner
            .optimized_ir_from_logical_plan(&foreign_plan)
            .await
            .unwrap();
        let mut foreign_analyses = foreign_planned.analyze();
        let foreign_estimate = foreign_analyses
            .get::<CardinalityEstimationV1>(
                &foreign_planned.query,
                foreign_planned.query.root().unwrap(),
            )
            .unwrap();
        assert_eq!(foreign_estimate.rows.value, 1000.0);

        let catalog = Catalog::new(db.clone());
        let reader = catalog.reader().await.unwrap();
        let relation_id = reader.get_all_relations().await.unwrap()[0].relation_id;
        catalog
            .invalidate_relation_statistics(reader.snapshot_id(), relation_id)
            .await
            .unwrap();
        let plan = runner
            .parse_logical_plan("SELECT * FROM numbers")
            .await
            .unwrap();
        let planned = runner.optimized_ir_from_logical_plan(&plan).await.unwrap();
        let root = planned.query.root().unwrap();
        let mut analyses = planned.analyze();
        let estimate = analyses
            .get::<CardinalityEstimationV1>(&planned.query, root)
            .unwrap();
        assert_eq!(estimate.rows.value, 1000.0);

        drop(catalog);
        drop(runner);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_advanced_statistics_do_not_replace_basic_statistics() {
        use optd_catalog::advanced_statistics::{
            AdvancedStatisticsContents, CreateAdvancedStatisticsInfo, DropAdvancedStatisticsInfo,
        };
        let (_temp_dir, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql("CREATE TABLE numbers AS SELECT CAST(1 AS BIGINT) AS x, CAST(2 AS BIGINT) AS y")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let catalog = Catalog::new(db.clone());
        let relation_id = catalog
            .reader()
            .await
            .unwrap()
            .get_all_relations()
            .await
            .unwrap()[0]
            .relation_id;
        let runner =
            OptdRunner::new_with_persistent_catalog(session.clone(), "datafusion", db.clone());
        let mut advanced_id = None;
        for (stage, expected) in [(0, 1000.0), (1, 1000.0), (2, 1.0), (3, 1.0)] {
            match stage {
                1 => {
                    advanced_id = Some(
                        catalog
                            .create_advanced_statistics(
                                catalog.reader().await.unwrap().snapshot_id(),
                                CreateAdvancedStatisticsInfo {
                                    relation_id,
                                    statistics_name: "xy_ndv".into(),
                                    column_ids: vec![0, 1],
                                    contents: AdvancedStatisticsContents::JointNdv {
                                        distinct_count: 999,
                                    },
                                },
                            )
                            .await
                            .unwrap(),
                    );
                }
                2 => {
                    session
                        .sql("SELECT collect_stats('numbers')")
                        .await
                        .unwrap()
                        .collect()
                        .await
                        .unwrap();
                }
                3 => {
                    catalog
                        .drop_advanced_statistics(
                            catalog.reader().await.unwrap().snapshot_id(),
                            DropAdvancedStatisticsInfo {
                                statistics_id: advanced_id.unwrap(),
                            },
                        )
                        .await
                        .unwrap();
                }
                _ => {}
            }
            let plan = runner
                .parse_logical_plan("SELECT * FROM numbers")
                .await
                .unwrap();
            let planned = runner.optimized_ir_from_logical_plan(&plan).await.unwrap();
            let mut analyses = planned.analyze();
            let estimate = analyses
                .get::<CardinalityEstimationV1>(&planned.query, planned.query.root().unwrap())
                .unwrap();
            assert_eq!(estimate.rows.value, expected, "stage {stage}");
        }
        drop(catalog);
        drop(runner);
        drop(session);
        db.close().await.unwrap();
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
        let catalog = std::sync::Arc::new(optd_core::MemoryCatalog::new("memory", "public"));
        let mut opt = optd_core::OptimizerContext::new(optd_core::QueryContext::new(), catalog);
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
    async fn physical_path_merges_distinct_aggregate_states_across_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = |values: &[i64]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1; values.len()])) as ArrayRef,
                    Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
                ],
            )
            .unwrap()
        };
        let table = MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch(&[10, 20])], vec![batch(&[20, 30])]],
        )
        .unwrap();
        let session = session_with_optd_config(OptdExtensionConfig {
            optd_enabled: true,
            log_explain_steps: false,
            physical_planning: true,
        });
        session.register_table("t", Arc::new(table)).unwrap();
        let runner = OptdRunner::new(session);

        let (_, batches, path) = runner
            .try_via_ir_with_path(
                "SELECT group_id, count(DISTINCT value) AS distinct_values \
                 FROM t GROUP BY group_id",
            )
            .await
            .unwrap();

        assert_eq!(path, IrExecutionPath::Physical);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        let distinct_values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(distinct_values.values(), &[3]);
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
