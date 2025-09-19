use std::sync::Arc;

use datafusion::{
    execution::{SessionStateBuilder, runtime_env::RuntimeEnv},
    prelude::{DataFrame, SessionConfig, SessionContext},
};
use datafusion_cli::cli_context::CliSessionContext;
use optd_datafusion::{OptdExtensionConfig, SessionStateBuilderOptdExt};

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
            // .with_optimizer_rules(Vec::new())
            .with_optd_planner()
            .build();
        let inner = SessionContext::new_with_state(state);

        Self { inner }
    }

    pub fn enable_url_table(self) -> Self {
        let inner = self.inner.enable_url_table();
        Self { inner }
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn return_empty_dataframe(&self) -> datafusion::common::Result<DataFrame> {
        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.inner.state(), plan))
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
                    Output = Result<
                        datafusion::prelude::DataFrame,
                        datafusion::common::DataFusionError,
                    >,
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
            }

            self.inner.execute_logical_plan(plan).await
        };

        Box::pin(fut)
    }
}
