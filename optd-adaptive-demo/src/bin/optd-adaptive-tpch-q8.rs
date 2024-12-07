// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog_common::MemoryCatalogProviderList;
use datafusion::error::Result;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_optd_cli::exec::{exec_from_commands, exec_from_commands_collect, exec_from_files};
use datafusion_optd_cli::print_format::PrintFormat;
use datafusion_optd_cli::print_options::{MaxRows, PrintOptions};
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut session_config = SessionConfig::from_env()?.with_information_schema(true);
    session_config.options_mut().optimizer.max_passes = 0;

    let rn_config = RuntimeConfig::new();
    let runtime_env = RuntimeEnv::try_new(rn_config.clone())?;

    let ctx = {
        let mut state = SessionStateBuilder::new()
            .with_config(session_config.clone())
            .with_runtime_env(Arc::new(runtime_env));
        let catalog = Arc::new(MemoryCatalogProviderList::new());
        let optimizer: DatafusionOptimizer = DatafusionOptimizer::new_physical(
            Arc::new(DatafusionCatalog::new(catalog.clone())),
            true,
        );
        state = state.with_catalog_list(catalog);
        // clean up optimizer rules so that we can plug in our own optimizer
        state = state.with_optimizer_rules(vec![]);
        state = state.with_physical_optimizer_rules(vec![]);
        // use optd-bridge query planner
        state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
        SessionContext::new_with_state(state.build())
    };
    ctx.refresh_catalogs().await?;

    let slient_print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: true,
        maxrows: MaxRows::Limited(5),
        color: false,
    };

    let print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
        maxrows: MaxRows::Limited(5),
        color: false,
    };

    exec_from_files(
        &ctx,
        vec!["datafusion-optd-cli/tpch-sf0_01/populate.sql".to_string()],
        &slient_print_options,
    )
    .await?;

    let mut iter = 0;

    loop {
        iter += 1;
        println!("--- ITERATION {} ---", iter);
        let sql = r#"
select
o_year,
sum(case
    when nation = 'IRAQ' then volume
    else 0
end) / sum(volume) as mkt_share
from
(
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
o_year
order by
o_year;
        "#;
        let result = exec_from_commands_collect(&ctx, vec![format!("explain {}", sql)]).await?;
        println!(
            "{}",
            result
                .iter()
                .find(|x| x[0] == "physical_plan after optd-join-order")
                .map(|x| &x[1])
                .unwrap()
        );
        exec_from_commands(&ctx, vec![sql.to_string()], &print_options).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
