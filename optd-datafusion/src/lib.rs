// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#![allow(clippy::new_without_default)]
use std::sync::Arc;

use datafusion::catalog::CatalogProviderList;
use datafusion::catalog_common::MemoryCatalogProviderList;
use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use planner::OptdOptimizer;
use planner::OptdQueryPlanner;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::Partitioning;
use futures::StreamExt;
use std::time::SystemTime;
pub mod converter;
pub mod planner;

pub async fn run_queries(queries: String) -> Result<()> {
    // Create a SessionContext with TPCH base tables
    let rt_config = RuntimeEnvBuilder::new();

    let session_config = SessionConfig::from_env()?.with_information_schema(true);

    let ctx = crate::create_df_context(Some(session_config.clone()), Some(rt_config.clone()), None)
        .await
        .unwrap();

    // Create a DataFrame with the input query
    for query in queries.split(';') {
        if query.trim().is_empty() {
            continue;
        }
        let sql = ctx.sql(query).await?;
        // Run our execution engine on the physical plan
        let df_physical_plan = sql.clone().create_physical_plan().await?;
        let plan = df_physical_plan.clone();
        println!("{:#?}", df_physical_plan.clone());
        // let df_physical_plan = df_physical_plan.children()[0].clone();
        let mut print_results: Vec<RecordBatch> = vec![];
        let now = SystemTime::now();

        // DataFusion execution nodes will output multiple streams that are partitioned by the following
        // patterns, so just join them all into one stream
        let partitions = match plan.output_partitioning() {
            Partitioning::RoundRobinBatch(c) => *c,
            Partitioning::Hash(_, h) => *h,
            Partitioning::UnknownPartitioning(p) => *p,
        };

        // In a separate tokio task, send batches to the next operator over the `tx` channel, and make
        // sure to make use of all of the partitions
        for i in 0..partitions {
            let batch_stream = plan.execute(i, Default::default()).unwrap();

            let results: Vec<_> = batch_stream.collect().await;
            for batch in results {
                let batch = batch.unwrap();
                if batch.num_rows() == 0 {
                    continue;
                }
                print_results.push(batch);
            }
        }

        match now.elapsed() {
            Ok(elapsed) => {
                // it prints '2'
                println!("Datafusion time in milliseconds: {}", elapsed.as_millis());
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }

        print_results.into_iter().for_each(|batch| {
            let pretty_results = pretty::pretty_format_batches(&[batch]).unwrap().to_string();
            println!("{}", pretty_results);
        });
    }
    Ok(())
}
/// Utility function to create a session context for datafusion + optd.
/// The allow deprecated is for the RuntimeConfig
#[allow(deprecated)]
pub async fn create_df_context(
    session_config: Option<SessionConfig>,
    rn_config: Option<RuntimeEnvBuilder>,
    catalog: Option<Arc<dyn CatalogProviderList>>,
) -> anyhow::Result<SessionContext> {
    let mut session_config = if let Some(session_config) = session_config {
        session_config
    } else {
        SessionConfig::from_env()?.with_information_schema(true)
    };

    // Disable Datafusion's heuristic rule based query optimizer
    session_config.options_mut().optimizer.max_passes = 0;

    let rn_config = if let Some(rn_config) = rn_config {
        rn_config
    } else {
        RuntimeEnvBuilder::new()
    };
    let runtime_env = Arc::new(rn_config.build()?);

    let catalog = if let Some(catalog) = catalog {
        catalog
    } else {
        Arc::new(MemoryCatalogProviderList::new())
    };

    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_catalog_list(catalog.clone())
        .with_default_features();

    let optimizer = OptdOptimizer {};
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
