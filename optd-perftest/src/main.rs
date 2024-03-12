use anyhow::Result;
use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use postgres_db::PostgresDb;

use crate::{
    benchmark::Benchmark,
    tpch::{TpchConfig, TPCH_KIT_POSTGRES},
};

mod benchmark;
mod cardtest;
mod datafusion_db_cardtest;
mod postgres_db;
mod shell;
mod tpch;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let pg_db = PostgresDb::build().await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db)];
    let tpch_config = TpchConfig {
        database: String::from(TPCH_KIT_POSTGRES),
        scale_factor: 1,
        seed: 15721,
    };
    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    let cardtest_runner = CardtestRunner::new(databases).await?;
    let qerrors = cardtest_runner
        .eval_benchmark_qerrors_alldbs(&tpch_benchmark)
        .await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
