mod extension;
mod planner;
mod table;

use core::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::plan_datafusion_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan};
use datafusion::optimizer as df_optimizer;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
pub use extension::{OptdExtension, OptdExtensionConfig};
pub use planner::OptdQueryPlanner;
pub use table::{OptdTable, OptdTableProvider};

pub use optd_core::error::Error as OptdError;
pub use optd_core::error::Result as OptdResult;
use optd_core::ir::catalog::Catalog;
use optd_core::ir::table_ref::TableRef as OptdTableRef;
use optd_core::magic::MemoryCatalog;
use optd_repository_api::optd_catalog::RepositoryCatalog;
use optd_repository_migration::{Migrator, MigratorTrait};
use sea_orm::Database;

pub trait SessionStateBuilderOptdExt: Sized {
    fn with_optd_planner(self) -> Self;
}

impl SessionStateBuilderOptdExt for datafusion::execution::SessionStateBuilder {
    fn with_optd_planner(self) -> Self {
        self.with_query_planner(Arc::new(OptdQueryPlanner::default()))
    }
}

pub fn default_datafusion_rules() -> Vec<Arc<dyn df_optimizer::OptimizerRule + Sync + Send>> {
    vec![
        Arc::new(df_optimizer::optimize_unions::OptimizeUnions::new()),
        Arc::new(df_optimizer::simplify_expressions::SimplifyExpressions::new()),
        Arc::new(df_optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate::new()),
        Arc::new(df_optimizer::eliminate_join::EliminateJoin::new()),
        Arc::new(df_optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery::new()),
        Arc::new(df_optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin::new()),
        Arc::new(df_optimizer::decorrelate_lateral_join::DecorrelateLateralJoin::new()),
        Arc::new(df_optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate::new()),
        Arc::new(df_optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr::new()),
        Arc::new(df_optimizer::eliminate_filter::EliminateFilter::new()),
        Arc::new(df_optimizer::eliminate_cross_join::EliminateCrossJoin::new()),
        Arc::new(df_optimizer::eliminate_limit::EliminateLimit::new()),
        Arc::new(df_optimizer::propagate_empty_relation::PropagateEmptyRelation::new()),
        Arc::new(df_optimizer::filter_null_join_keys::FilterNullJoinKeys::default()),
        Arc::new(df_optimizer::eliminate_outer_join::EliminateOuterJoin::new()),
        // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        Arc::new(df_optimizer::push_down_limit::PushDownLimit::new()),
        Arc::new(df_optimizer::push_down_filter::PushDownFilter::new()),
        Arc::new(df_optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy::new()),
        // The previous optimizations added expressions and projections,
        // that might benefit from the following rules
        Arc::new(df_optimizer::eliminate_group_by_constant::EliminateGroupByConstant::new()),
        Arc::new(df_optimizer::common_subexpr_eliminate::CommonSubexprEliminate::new()),
        // Arc::new(df_optimizer::optimize_projections::OptimizeProjections::new()),
    ]
}

pub fn create_optd_session_context(
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
) -> SessionContext {
    create_optd_session_context_with_catalog(
        config,
        runtime,
        Arc::new(MemoryCatalog::new("datafusion", "public")),
    )
}

pub fn memory_catalog() -> Arc<dyn Catalog> {
    Arc::new(MemoryCatalog::new("datafusion", "public"))
}

pub async fn repository_catalog_from_url(
    database_url: &str,
) -> Result<Arc<dyn Catalog>, DataFusionError> {
    let db = Database::connect(database_url)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    Migrator::up(&db, None)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    Ok(Arc::new(RepositoryCatalog::new(db, "datafusion", "public")))
}

pub fn create_optd_session_context_with_catalog(
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
    catalog: Arc<dyn Catalog>,
) -> SessionContext {
    let optd_extension = Arc::new(OptdExtension::new(catalog));

    let config = config
        .with_option_extension(OptdExtensionConfig::default())
        .with_extension(optd_extension)
        .set_bool("optd.optd_enabled", true)
        .set_bool("optd.optd_strict_mode", false);
    // disable datafusion logical optimizer.
    // .set_usize("datafusion.optimizer.max_passes", 0);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_optimizer_rules(default_datafusion_rules())
        .with_optd_planner()
        .build();
    SessionContext::new_with_state(state)
}

pub struct OptdSessionContext {
    inner: SessionContext,
}

pub struct ProperlyFormattedCreateExternalTable<'a>(&'a CreateExternalTable);

impl<'a> fmt::Display for ProperlyFormattedCreateExternalTable<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL TABLE ")?;
        if self.0.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", self.0.name)?;
        write!(f, "STORED AS {} ", self.0.file_type)?;
        if !self.0.order_exprs.is_empty() {
            write!(f, "WITH ORDER (")?;
            let mut first = true;
            for expr in self.0.order_exprs.iter().flatten() {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "{expr}")?;
                first = false;
            }
            write!(f, ") ")?;
        }
        write!(f, "LOCATION '{}'", self.0.location)
    }
}

impl OptdSessionContext {
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            inner: create_optd_session_context(config, runtime),
        }
    }

    pub fn new_with_config_rt_catalog(
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        catalog: Arc<dyn Catalog>,
    ) -> Self {
        Self {
            inner: create_optd_session_context_with_catalog(config, runtime, catalog),
        }
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub async fn refresh_catalogs(&self) -> datafusion::common::Result<()> {
        self.inner.refresh_catalogs().await
    }

    pub fn enable_url_table(self) -> Self {
        let inner = self.inner.enable_url_table();
        Self { inner }
    }

    pub async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError> {
        let df = self.inner.execute_logical_plan(plan.clone()).await?;
        self.sync_optd_catalog_from_plan(&plan).await?;
        Ok(df)
    }

    async fn sync_optd_catalog_from_plan(&self, plan: &LogicalPlan) -> Result<(), DataFusionError> {
        if let LogicalPlan::Ddl(ddl) = plan {
            match ddl {
                DdlStatement::CreateExternalTable(create_table) => {
                    let definition = ProperlyFormattedCreateExternalTable(create_table).to_string();

                    self.create_table_from_registered_provider(
                        create_table.name.clone(),
                        Some(definition),
                    )
                    .await
                }
                DdlStatement::CreateMemoryTable(create_table) => {
                    self.create_table_from_registered_provider(create_table.name.clone(), None)
                        .await
                }
                DdlStatement::DropTable(drop_table) => self.drop_table(drop_table.name.clone()),
                _ => Ok(()),
            }?;
        }

        Ok(())
    }

    fn create_table(
        &self,
        table_ref: impl Into<TableReference>,
        schema: datafusion::arrow::datatypes::SchemaRef,
        definition: Option<String>,
    ) -> Result<(), DataFusionError> {
        let table_ref: TableReference = table_ref.into();
        let optd_table_ref = Self::into_optd_table_ref(&table_ref);
        let state = self.inner.state();
        let extension = state
            .config()
            .get_extension::<OptdExtension>()
            .ok_or_else(|| DataFusionError::Execution("Missing optd session extension".into()))?;

        extension
            .catalog()
            .create_table(optd_table_ref, schema, definition)
            .map_err(|e| {
                DataFusionError::External(Box::new(optd_core::error::Error::Catalog { source: e }))
            })?;

        Ok(())
    }

    async fn create_table_from_registered_provider(
        &self,
        table_ref: impl Into<TableReference>,
        definition: Option<String>,
    ) -> Result<(), DataFusionError> {
        let table_ref: TableReference = table_ref.into();
        let provider = self.inner.table_provider(table_ref.clone()).await?;
        self.create_table(table_ref, provider.schema(), definition)
    }

    fn drop_table(&self, table_ref: impl Into<TableReference>) -> Result<(), DataFusionError> {
        let table_ref: TableReference = table_ref.into();
        let optd_table_ref = Self::into_optd_table_ref(&table_ref);
        let state = self.inner.state();
        let extension = state
            .config()
            .get_extension::<OptdExtension>()
            .ok_or_else(|| DataFusionError::Execution("Missing optd session extension".into()))?;

        extension
            .catalog()
            .drop_table(optd_table_ref)
            .map_err(|e| {
                DataFusionError::External(Box::new(optd_core::error::Error::Catalog { source: e }))
            })?;

        Ok(())
    }

    fn into_optd_table_ref(table_ref: &TableReference) -> OptdTableRef {
        match table_ref {
            TableReference::Bare { table } => OptdTableRef::bare(table.clone()),
            TableReference::Partial { schema, table } => {
                OptdTableRef::partial(schema.clone(), table.clone())
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => OptdTableRef::full(catalog.clone(), schema.clone(), table.clone()),
        }
    }
}

pub struct DataFusionDB {
    ctx: OptdSessionContext,
}

impl DataFusionDB {
    pub async fn new() -> Result<Self, DataFusionError> {
        let config_options = ConfigOptions::from_env()?;
        let config = SessionConfig::from(config_options).with_information_schema(true);

        let ctx = OptdSessionContext::new_with_config_rt(config, Arc::new(RuntimeEnv::default()));
        Ok(Self { ctx })
    }

    pub async fn new_with_catalog(catalog: Arc<dyn Catalog>) -> Result<Self, DataFusionError> {
        let config_options = ConfigOptions::from_env()?;
        let config = SessionConfig::from(config_options).with_information_schema(true);

        let ctx = OptdSessionContext::new_with_config_rt_catalog(
            config,
            Arc::new(RuntimeEnv::default()),
            catalog,
        );
        Ok(Self { ctx })
    }

    pub fn session_context(&self) -> &SessionContext {
        self.ctx.inner()
    }

    pub async fn execute_one(&self, sql: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.execute_inner(sql, true).await
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.execute_inner(sql, false).await
    }

    pub async fn execute_inner(
        &self,
        sql: &str,
        single_stmt_check: bool,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = self.ctx.inner().task_ctx();
        let options = task_ctx.session_config().options();
        let dialect = &options.sql_parser.dialect;

        let statements = {
            let dialect = dialect_from_str(dialect).ok_or_else(|| {
                plan_datafusion_err!(
                    "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks."
                )
            })?;
            DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?
        };

        if single_stmt_check && statements.len() != 1 {
            panic!("Only support executing one statement at a time")
        }

        let mut results = Vec::new();
        for statement in statements {
            let plan = self
                .ctx
                .inner()
                .state()
                .statement_to_plan(statement)
                .await?;
            let df = self.ctx.execute_logical_plan(plan).await?;
            let task_ctx = Arc::new(df.task_ctx());
            let physical_plan = df.create_physical_plan().await?;
            let batches = datafusion::physical_plan::collect(physical_plan, task_ctx).await?;
            results.extend(batches);
        }
        Ok(results)
    }
}
