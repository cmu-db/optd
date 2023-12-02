use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_optd_cli::exec::{exec_from_commands_collect, exec_from_files};
use datafusion_optd_cli::{
    exec::exec_from_commands,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
};
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use std::sync::Arc;
use std::time::Duration;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let session_config = SessionConfig::from_env()?.with_information_schema(true);

    let rn_config = RuntimeConfig::new();
    let runtime_env = RuntimeEnv::new(rn_config.clone())?;

    let mut ctx = {
        let mut state =
            SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
        let optimizer = DatafusionOptimizer::new_physical(Box::new(DatafusionCatalog::new(
            state.catalog_list(),
        )));
        state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
        SessionContext::new_with_state(state)
    };
    ctx.refresh_catalogs().await?;

    let slient_print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: true,
        maxrows: MaxRows::Limited(5),
    };

    let print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
        maxrows: MaxRows::Limited(5),
    };

    exec_from_files(
        vec!["tpch/populate.sql".to_string()],
        &mut ctx,
        &slient_print_options,
    )
    .await;

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
        let result = exec_from_commands_collect(&mut ctx, vec![format!("explain {}", sql)]).await?;
        println!(
            "{}",
            result
                .iter()
                .find(|x| x[0] == "physical_plan after optd-join-order")
                .map(|x| &x[1])
                .unwrap()
        );
        exec_from_commands(&mut ctx, &print_options, vec![sql.to_string()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
