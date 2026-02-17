mod catalog;
mod extension;
mod planner;
mod table;
mod value;

use std::sync::Arc;

pub use catalog::{OptdCatalogProvider, OptdCatalogProviderList, OptdSchemaProvider};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::plan_datafusion_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
pub use extension::{OptdExtension, OptdExtensionConfig};
pub use planner::OptdQueryPlanner;
pub use table::{OptdTable, OptdTableProvider};

pub use optd_core::error::Error as OptdError;
pub use optd_core::error::Result as OptdResult;

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
    let config = config
        .with_option_extension(OptdExtensionConfig::default())
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

pub struct DataFusionDB {
    ctx: SessionContext,
}

impl DataFusionDB {
    pub async fn new() -> Result<Self, DataFusionError> {
        let config_options = ConfigOptions::from_env()?;
        let config = SessionConfig::from(config_options).with_information_schema(true);

        let ctx = create_optd_session_context(config, Arc::new(RuntimeEnv::default()));
        Ok(Self { ctx })
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
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
        let task_ctx = self.ctx.task_ctx();
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
            DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?
        };

        if single_stmt_check && statements.len() != 1 {
            panic!("Only support executing one statement at a time")
        }

        let mut results = Vec::new();
        for statement in statements {
            let plan = self.ctx.state().statement_to_plan(statement).await?;
            let df = self.ctx.execute_logical_plan(plan).await?;
            let task_ctx = Arc::new(df.task_ctx());
            let physical_plan = df.create_physical_plan().await?;
            let batches = datafusion::physical_plan::collect(physical_plan, task_ctx).await?;
            results.extend(batches);
        }
        Ok(results)
    }
}
