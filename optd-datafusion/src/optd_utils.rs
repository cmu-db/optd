use datafusion::catalog::{CatalogProviderList, MemoryCatalogProviderList};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

use crate::planner::OptdOptimizer;
use crate::planner::OptdQueryPlanner;

/// Utility function to create a session context for datafusion + optd.
/// TODO docs.
pub(crate) async fn create_optd_session(
    session_config: Option<SessionConfig>,
    runtime_env: Option<Arc<RuntimeEnv>>,
    datafusion_catalog: Option<Arc<dyn CatalogProviderList>>,
) -> anyhow::Result<SessionContext> {
    let mut session_config = match session_config {
        Some(config) => config,
        None => SessionConfig::from_env()?.with_information_schema(true),
    };

    // Disable Datafusion's heuristic rule based query optimizer
    session_config.options_mut().optimizer.max_passes = 0;

    let runtime_env = match runtime_env {
        Some(runtime_env) => runtime_env,
        None => Arc::new(RuntimeEnvBuilder::new().build()?),
    };

    let catalog = match datafusion_catalog {
        Some(catalog) => catalog,
        None => Arc::new(MemoryCatalogProviderList::new()),
    };

    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_catalog_list(catalog.clone())
        .with_default_features();

    let optimizer = OptdOptimizer::new_in_memory().await?;
    let planner = Arc::new(OptdQueryPlanner::new(optimizer));
    // clean up optimizer rules so that we can plug in our own optimizer
    builder = builder.with_optimizer_rules(vec![]);
    builder = builder.with_physical_optimizer_rules(vec![]);

    // use optd-bridge query planner
    builder = builder.with_query_planner(planner);

    let state = builder.build();
    let ctx = SessionContext::new_with_state(state).enable_url_table();
    ctx.refresh_catalogs().await?;
    Ok(ctx)
}
