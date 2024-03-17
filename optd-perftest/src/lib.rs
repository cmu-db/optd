use std::path::Path;

use crate::{benchmark::Benchmark, datafusion_db_cardtest::DatafusionDb, tpch::TpchConfig};
use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use postgres_db::PostgresDb;

mod benchmark;
mod cardtest;
mod datafusion_db_cardtest;
mod postgres_db;
pub mod shell;
pub mod tpch;

pub async fn cardtest<P: AsRef<Path> + Clone>(
    workspace_dpath: P,
    tpch_config: TpchConfig,
) -> anyhow::Result<()> {
    let pg_db = PostgresDb::new(workspace_dpath.clone());
    let df_db = DatafusionDb::new(workspace_dpath).await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db), Box::new(df_db)];

    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    let mut cardtest_runner = CardtestRunner::new(databases).await?;
    let qerrors = cardtest_runner
        .eval_benchmark_qerrors_alldbs(&tpch_benchmark)
        .await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
