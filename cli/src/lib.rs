use std::sync::Arc;
use datafusion::{
    common::{exec_err, not_impl_err, DataFusionError, Result}, 
    datasource::TableProvider, 
    logical_expr::{CreateExternalTable, LogicalPlanBuilder}, 
    prelude::{DataFrame, SessionContext}, 
    sql::TableReference
};
use datafusion_cli::cli_context::CliSessionContext;
use tokio::sync::RwLock;
use optd_catalog::OptdSchemaProvider;

pub struct OptdCliSessionContext {
    inner: SessionContext,
}

impl OptdCliSessionContext {
    pub fn new(inner: SessionContext) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    #[ignore = "not yet fully implemented"]
    // pub fn register_optd_catalog(&self, optd_catalog: Arc<OptdCatalogProviderList>) -> Result<()> {
    //     let state = self.inner.state_ref().read().clone();
        // state.register_catalog(
        //     "ducklake",
        //     Arc::new(datafusion_ducklake::DuckLakeCatalogProvider::new()),
        // )
    // }

    pub fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.inner.state(), plan))
    }

    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<DataFrame> {
        let exist = self.inner.table_exist(cmd.name.clone())?;

        if cmd.temporary {
            return not_impl_err!("Temporary tables not supported");
        }

        if exist {
            match cmd.if_not_exists {
                true => return self.return_empty_dataframe(),
                false => {
                    return exec_err!("Table '{}' already exists", cmd.name);
                }
            }
        }

        let table_provider: Arc<dyn TableProvider> =
            self.create_custom_table(cmd).await?;
        self.register_table(cmd.name.clone(), table_provider)?;

        self.return_empty_dataframe()
    }

    async fn create_custom_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.inner.state_ref().read().clone();
        let file_type = cmd.file_type.to_uppercase();
        let factory =
            state
                .table_factories()
                .get(file_type.as_str())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Unable to find factory for {}",
                        cmd.file_type
                    ))
                })?;
        let table = (*factory).create(&state, cmd).await?;
        Ok(table)
    }

    pub fn register_table(
        &self,
        table_ref: impl Into<TableReference>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref: TableReference = table_ref.into();
        let table = table_ref.table().to_owned();
        self.inner.state_ref()
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table, provider)
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
            dyn ::core::future::Future<
                    Output = Result<DataFrame, DataFusionError>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let fut = async {
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
                        return self.create_external_table(&create_table).await;
                    }
                    _ => (),
                }
            }
            self.inner.execute_logical_plan(plan).await
        };

        Box::pin(fut)
    }
}