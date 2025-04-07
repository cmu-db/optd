use crate::mock::optimizer::MockOptimizer;
use datafusion::catalog::{CatalogProviderList, MemoryCatalogProviderList};
use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::Catalog;
use std::sync::Arc;

/// Creates a DataFusion `SessionContext` with the given optional parameters that uses `optd` as the
/// query planner and disables any optimizations that DataFusion itself performs.
pub(crate) async fn create_optd_session(
    session_config: Option<SessionConfig>,
    runtime_env: Option<Arc<RuntimeEnv>>,
    datafusion_catalog: Option<Arc<dyn CatalogProviderList>>,
) -> Result<SessionContext> {
    // Use the provided session configuration or create one from the environment variables.
    let session_config = {
        let mut config = session_config
            .unwrap_or_else(|| {
                SessionConfig::from_env().expect("Failed to create session config from env")
            })
            .with_information_schema(true);

        // Disable DataFusion's heuristic rule-based optimizer by setting the passes to 1.
        config.options_mut().optimizer.max_passes = 0;
        config
    };

    // Use the provided runtime environment or create the default one.
    let runtime_env =
        runtime_env.unwrap_or_else(|| Arc::new(RuntimeEnvBuilder::new().build().unwrap()));

    // Use the provided catalog or create a default one.
    let datafusion_catalog =
        datafusion_catalog.unwrap_or_else(|| Arc::new(MemoryCatalogProviderList::new()));

    // Use the `optd` optimizer as the query planner instead of the default one.
    let optimizer = MockOptimizer::new().await;

    // Build up the state for the `SessionContext`. Removes all optimizer rules so that it
    // completely relies on `optd`.
    let session_state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_catalog_list(datafusion_catalog.clone())
        .with_default_features()
        .with_optimizer_rules(vec![])
        .with_physical_optimizer_rules(vec![])
        .with_query_planner(Arc::new(optimizer))
        .build();

    // Create the `SessionContext` and refresh the catalogs to ensure everything is up-to-date.
    let ctx = SessionContext::new_with_state(session_state).enable_url_table();
    ctx.refresh_catalogs().await?;

    Ok(ctx)
}
