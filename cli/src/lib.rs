pub mod auto_stats;
pub mod udtf;

use datafusion::{
    common::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    logical_expr::LogicalPlanBuilder,
    prelude::{DataFrame, SessionConfig, SessionContext},
    sql::TableReference,
};
use datafusion_cli::cli_context::CliSessionContext;
use optd_core::ir::table_ref::TableRef as OptdTableRef;
use optd_datafusion::{OptdExtension, create_optd_session_context};
use std::sync::Arc;

/// The optd CLI session context for executing queries in DataFusion
/// with optd features.
pub struct OptdCliSessionContext {
    inner: SessionContext,
}

impl OptdCliSessionContext {
    /// Creates a new `OptdCliSessionContext` with the given DataFusion session config
    /// and a runtime environment.
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            inner: create_optd_session_context(config, runtime),
        }
    }

    /// Refreshes catalogs.
    pub async fn refresh_catalogs(&self) -> datafusion::common::Result<()> {
        self.inner.refresh_catalogs().await
    }

    /// Enables querying local files as tables.
    pub fn enable_url_table(self) -> Self {
        let inner = self.inner.enable_url_table();
        Self { inner }
    }

    /// Returns a reference to the inner `SessionContext`.
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Returns an empty DataFrame.
    pub fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.inner.state(), plan))
    }

    fn create_table(
        &self,
        table_ref: impl Into<TableReference>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<()> {
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

    fn drop_table(&self, table_ref: impl Into<TableReference>) -> Result<()> {
        let table_ref: TableReference = table_ref.into();
        let optd_table_ref = Self::into_optd_table_ref(&table_ref);
        let state = self.inner.state();
        let extension = state
            .config()
            .get_extension::<OptdExtension>()
            .ok_or_else(|| DataFusionError::Execution("Missing optd session extension".into()))?;

        extension.catalog().drop_table(optd_table_ref).map_err(|e| {
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

impl CliSessionContext for OptdCliSessionContext {
    fn task_ctx(&self) -> std::sync::Arc<datafusion::execution::TaskContext> {
        self.inner().task_ctx()
    }

    fn session_state(&self) -> datafusion::execution::SessionState {
        self.inner().state()
    }

    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
    ) -> Option<std::sync::Arc<dyn object_store::ObjectStore + 'static>> {
        self.inner().register_object_store(url, object_store)
    }

    fn register_table_options_extension_from_scheme(&self, scheme: &str) {
        self.inner()
            .register_table_options_extension_from_scheme(scheme);
    }

    fn execute_logical_plan<'life0, 'async_trait>(
        &'life0 self,
        plan: datafusion::logical_expr::LogicalPlan,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<DataFrame, DataFusionError>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let fut = async move {
            let df = self.inner.execute_logical_plan(plan.clone()).await?;
            if let datafusion::logical_expr::LogicalPlan::Statement(stmt) = &plan {
                match stmt {
                    datafusion::logical_expr::Statement::TransactionStart(_) => {
                        println!("START TRANSACTION");
                        return self.return_empty_dataframe();
                    }
                    datafusion::logical_expr::Statement::TransactionEnd(transaction_end) => {
                        use datafusion::logical_expr::TransactionConclusion;
                        match transaction_end.conclusion {
                            TransactionConclusion::Commit => println!("COMMIT"),
                            TransactionConclusion::Rollback => println!("ROLLBACK"),
                        }
                        return self.return_empty_dataframe();
                    }
                    _ => (),
                }
            } else if let datafusion::logical_expr::LogicalPlan::Ddl(ddl) = &plan {
                match ddl {
                    datafusion::logical_expr::DdlStatement::CreateExternalTable(create_table) => {
                        self.create_table(
                            create_table.name.clone(),
                            create_table.schema.inner().clone(),
                        )?;
                    }
                    datafusion::logical_expr::DdlStatement::CreateMemoryTable(create_table) => {
                        let schema = create_table.input.schema();
                        self.create_table(create_table.name.clone(), schema.inner().clone())?;
                    }
                    datafusion::logical_expr::DdlStatement::DropTable(drop_table) => {
                        self.drop_table(drop_table.name.clone())?;
                    }
                    _ => (),
                }
            }
            Ok(df)
        };

        Box::pin(fut)
    }
}
