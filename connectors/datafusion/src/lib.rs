mod catalog;
mod extension;
mod planner;
mod table;

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::plan_datafusion_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
pub use extension::{OptdExtension, OptdExtensionConfig};
pub use planner::OptdQueryPlanner;
pub use table::{OptdTable, OptdTableProvider};

pub use optd_core::error::Error as OptdError;
pub use optd_core::error::Result as OptdResult;
use optd_core::ir::table_ref::TableRef as OptdTableRef;

pub trait SessionStateBuilderOptdExt: Sized {
    fn with_optd_planner(self) -> Self;
}

impl SessionStateBuilderOptdExt for datafusion::execution::SessionStateBuilder {
    fn with_optd_planner(self) -> Self {
        self.with_query_planner(Arc::new(OptdQueryPlanner::default()))
    }
}

pub fn create_optd_session_context(
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
) -> SessionContext {
    let optd_extension = Arc::new(OptdExtension::default());
    let config = config
        .with_option_extension(OptdExtensionConfig::default())
        .with_extension(optd_extension)
        .set_bool("optd.optd_enabled", true)
        .set_bool("optd.optd_strict_mode", false);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_optd_planner()
        .build();
    SessionContext::new_with_state(state)
}

pub struct OptdSessionContext {
    inner: SessionContext,
}

impl OptdSessionContext {
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            inner: create_optd_session_context(config, runtime),
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
        self.sync_optd_catalog_from_plan(&plan)?;
        Ok(df)
    }

    fn sync_optd_catalog_from_plan(&self, plan: &LogicalPlan) -> Result<(), DataFusionError> {
        if let LogicalPlan::Ddl(ddl) = plan {
            match ddl {
                DdlStatement::CreateExternalTable(create_table) => self.create_table(
                    create_table.name.clone(),
                    create_table.schema.inner().clone(),
                ),
                DdlStatement::CreateMemoryTable(create_table) => {
                    let schema = create_table.input.schema();
                    self.create_table(create_table.name.clone(), schema.inner().clone())
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
            .create_table(optd_table_ref, schema)
            .map_err(|e| {
                DataFusionError::External(Box::new(optd_core::error::Error::Catalog { source: e }))
            })?;

        Ok(())
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
