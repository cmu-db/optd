use datafusion::{
    arrow::{array::{RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}},
    catalog::CatalogProviderList,
    common::{DataFusionError, Result, exec_err, not_impl_err},
    datasource::TableProvider,
    execution::{SessionStateBuilder, runtime_env::RuntimeEnv},
    logical_expr::{CreateExternalTable, LogicalPlanBuilder},
    prelude::{DataFrame, SessionConfig, SessionContext},
    sql::TableReference,
};
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogServiceHandle, RegisterTableRequest};
use optd_datafusion::{
    OptdCatalogProvider, OptdCatalogProviderList, OptdExtensionConfig, SessionStateBuilderOptdExt,
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct OptdCliSessionContext {
    inner: SessionContext,
}

impl OptdCliSessionContext {
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let config = config
            .with_option_extension(OptdExtensionConfig::default())
            .set_bool("optd.optd_enabled", false);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_optd_planner()
            .build();
        let inner = SessionContext::new_with_state(state);

        Self { inner }
    }
    pub async fn refresh_catalogs(&self) -> datafusion::common::Result<()> {
        self.inner.refresh_catalogs().await
    }

    pub fn enable_url_table(self) -> Self {
        let inner = self.inner.enable_url_table();
        Self { inner }
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.inner.state(), plan))
    }

    /// Execute SQL statement, intercepting SHOW TABLES and other custom commands.
    pub async fn sql_with_optd(&self, sql: &str) -> Result<DataFrame> {
        // Check if this is a SHOW TABLES command
        let sql_trimmed = sql.trim().to_uppercase();
        if sql_trimmed.starts_with("SHOW TABLES") || sql_trimmed == "SHOW TABLES;" {
            return self.show_tables().await;
        }

        // Otherwise, use standard DataFusion SQL execution
        self.inner.sql(sql).await
    }

    async fn create_external_table(&self, cmd: &CreateExternalTable) -> Result<DataFrame> {
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

        let table_provider: Arc<dyn TableProvider> = self.create_custom_table(cmd).await?;
        self.register_table(cmd.name.clone(), table_provider)?;

        // Persist metadata to catalog if OptD catalog is enabled.
        if let Some(catalog_handle) = self.get_catalog_handle() {
            let request = RegisterTableRequest {
                table_name: cmd.name.to_string(),
                schema_name: None, // Use default schema
                location: cmd.location.clone(),
                file_format: cmd.file_type.clone(),
                compression: Self::extract_compression(&cmd.options),
                options: cmd.options.clone(),
            };

            catalog_handle
                .register_external_table(request)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        self.return_empty_dataframe()
    }

    async fn create_custom_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.inner.state_ref().read().clone();
        let file_type = cmd.file_type.to_uppercase();
        let factory = state
            .table_factories()
            .get(file_type.as_str())
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Unable to find factory for {}", cmd.file_type))
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
        self.inner
            .state_ref()
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table, provider)
    }

    /// Extracts the catalog handle from the wrapped catalog list.
    fn get_catalog_handle(&self) -> Option<CatalogServiceHandle> {
        let state = self.inner.state();
        let catalog_list = state.catalog_list();

        let optd_list = catalog_list
            .as_any()
            .downcast_ref::<OptdCatalogProviderList>()?;

        let catalog = optd_list.catalog("datafusion")?;

        let optd_catalog = catalog.as_any().downcast_ref::<OptdCatalogProvider>()?;

        optd_catalog.catalog_handle().cloned()
    }

    /// Extracts compression option from `CreateExternalTable` options.
    fn extract_compression(options: &HashMap<String, String>) -> Option<String> {
        options
            .get("format.compression")
            .or_else(|| options.get("compression"))
            .cloned()
    }

    /// Handles DROP TABLE command by deregistering from DataFusion and persisting the deletion.
    async fn drop_external_table(&self, table_name: &str, if_exists: bool) -> Result<DataFrame> {
        // Check if table exists in DataFusion.
        let table_exists = self
            .inner
            .state()
            .catalog_list()
            .catalog("datafusion")
            .and_then(|cat| cat.schema("public"))
            .map(|schema| schema.table_exist(table_name))
            .unwrap_or(false);

        if !table_exists {
            if if_exists {
                return self.return_empty_dataframe();
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Table '{}' doesn't exist",
                    table_name
                )));
            }
        }

        // Deregister from DataFusion's in-memory catalog.
        self.inner
            .state()
            .catalog_list()
            .catalog("datafusion")
            .and_then(|cat| cat.schema("public"))
            .and_then(|schema| schema.deregister_table(table_name).ok())
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Failed to deregister table '{}'", table_name))
            })?;

        if let Some(catalog_handle) = self.get_catalog_handle() {
            catalog_handle
                .drop_external_table(None, table_name)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        self.return_empty_dataframe()
    }

    /// Lists all registered external tables from the catalog.
    ///
    /// Returns a DataFrame with table metadata including name, location, format, and compression.
    async fn show_tables(&self) -> Result<DataFrame> {
        if let Some(catalog_handle) = self.get_catalog_handle() {
            let tables = catalog_handle
                .list_external_tables(None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Create schema for result
            let schema = Arc::new(Schema::new(vec![
                Field::new("table_name", DataType::Utf8, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("file_format", DataType::Utf8, false),
                Field::new("compression", DataType::Utf8, true),
            ]));

            // Build arrays from table metadata
            let table_names: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();
            let locations: Vec<String> = tables.iter().map(|t| t.location.clone()).collect();
            let formats: Vec<String> = tables.iter().map(|t| t.file_format.clone()).collect();
            let compressions: Vec<Option<String>> =
                tables.iter().map(|t| t.compression.clone()).collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(table_names)),
                    Arc::new(StringArray::from(locations)),
                    Arc::new(StringArray::from(formats)),
                    Arc::new(StringArray::from(compressions)),
                ],
            )?;

            // Convert to DataFrame
            self.inner
                .read_batch(batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            // No catalog handle available - return empty result
            let schema = Arc::new(Schema::new(vec![
                Field::new("table_name", DataType::Utf8, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("file_format", DataType::Utf8, false),
                Field::new("compression", DataType::Utf8, true),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(Vec::<String>::new())),
                    Arc::new(StringArray::from(Vec::<String>::new())),
                    Arc::new(StringArray::from(Vec::<String>::new())),
                    Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                ],
            )?;

            self.inner
                .read_batch(batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))
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
                        return self.create_external_table(create_table).await;
                    }
                    datafusion::logical_expr::DdlStatement::DropTable(drop_table) => {
                        let table_name = drop_table.name.to_string();
                        return self
                            .drop_external_table(&table_name, drop_table.if_exists)
                            .await;
                    }
                    _ => (),
                }
            }
            self.inner.execute_logical_plan(plan).await
        };

        Box::pin(fut)
    }
}
