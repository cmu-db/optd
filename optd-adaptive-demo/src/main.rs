use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_optd_cli::{
    exec::exec_from_commands,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
};
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use rand::Rng;
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

    exec_from_commands(
        &mut ctx,
        &slient_print_options,
        vec![
            "create table t1(t1v1 int, t1v2 int);".to_string(),
            "create table t2(t2v1 int);".to_string(),
            "create table t3(t3v2 int);".to_string(),
        ],
    )
    .await;

    let mut data_progress = vec![50; 3];
    let mut iter = 0;

    fn do_insert(table: usize, begin: usize, end: usize, repeat: usize) -> String {
        let table_name = match table {
            0 => "t1",
            1 => "t2",
            2 => "t3",
            _ => unreachable!(),
        };
        let values = (begin..end)
            .collect::<Vec<_>>()
            .repeat(repeat)
            .into_iter()
            .map(|i| {
                if table == 0 {
                    format!("({}, {})", i, i)
                } else {
                    format!("({})", i)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!("insert into {} values {}", table_name, values);
        statement
    }
    let statement = do_insert(0, 0, 50, 1);
    exec_from_commands(&mut ctx, &slient_print_options, vec![statement]).await;
    let statement = do_insert(1, 0, 50, 1);
    exec_from_commands(&mut ctx, &slient_print_options, vec![statement]).await;
    let statement = do_insert(2, 0, 50, 1);
    exec_from_commands(&mut ctx, &slient_print_options, vec![statement]).await;
    loop {
        for table in 0..3 {
            let progress = rand::thread_rng().gen_range(5..10);
            let repeat = rand::thread_rng().gen_range(1..=3);
            let begin = data_progress[table];
            let end = begin + progress;
            data_progress[table] = end;
            let statement = do_insert(table, begin, end, repeat);
            exec_from_commands(&mut ctx, &slient_print_options, vec![statement]).await;
        }
        iter += 1;
        println!("--- ITERATION {} ---", iter);
        exec_from_commands(
            &mut ctx,
            &print_options,
            vec!["explain select * from t2, t1, t3 where t1v1 = t2v1 and t1v2 = t3v2;".to_string()],
        )
        .await;
        exec_from_commands(
            &mut ctx,
            &slient_print_options,
            vec!["select * from t2, t1, t3 where t1v1 = t2v1 and t1v2 = t3v2;".to_string()],
        )
        .await;
        exec_from_commands(
            &mut ctx,
            &slient_print_options,
            vec!["select (select count(*) from t1) as t1cnt, (select count(*) from t2) as t2cnt, (select count(*) from t3) as t3cnt;".to_string()],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
