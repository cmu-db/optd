use anyhow::Result;
use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;

use crate::{
    benchmark::Benchmark,
    tpch::{TpchConfig, TpchKit, TPCH_KIT_POSTGRES},
};

mod benchmark;
mod cardtest;
mod datafusion_db_cardtest;
mod postgres_db;
mod shell;
mod tpch;

#[tokio::main]
async fn main() -> Result<()> {
    let pg_db = PostgresDb::build(true).await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db)];
    let cardtest_runner = CardtestRunner::new(databases).await?;
    let tpch_cfg = TpchConfig {
        database: String::from(TPCH_KIT_POSTGRES),
        scale_factor: 1,
        seed: 15721,
    };
    let tpch_benchmark = Benchmark::Tpch(tpch_cfg.clone());
    cardtest_runner.load_databases(tpch_benchmark).await?;
    let qerrors = cardtest_runner.eval_qerrors("SELECT * FROM region;").await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
