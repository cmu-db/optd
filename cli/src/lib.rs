pub mod auto_stats;
pub mod udtf;

use datafusion::{
    common::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    logical_expr::LogicalPlanBuilder,
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SessionConfig, SessionContext},
};
use datafusion_cli::cli_context::CliSessionContext;
use optd_datafusion::OptdSessionContext;
use std::sync::Arc;

/// The optd CLI session context for executing queries in DataFusion
/// with optd features.
pub struct OptdCliSessionContext {
    inner: OptdSessionContext,
}

impl OptdCliSessionContext {
    /// Creates a new `OptdCliSessionContext` with the given DataFusion session config
    /// and a runtime environment.
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            inner: OptdSessionContext::new_with_config_rt(config, runtime),
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
        self.inner.inner()
    }

    /// Returns an empty DataFrame.
    pub fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.inner().state(), plan))
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
            if let LogicalPlan::Statement(stmt) = &plan {
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
            }
            Ok(df)
        };

        Box::pin(fut)
    }
}
