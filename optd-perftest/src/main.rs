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
    let tpch_cfg = TpchConfig {
        database: String::from(TPCH_KIT_POSTGRES),
        scale_factor: 1,
        seed: 15721,
    };
    let tpch_benchmark = Benchmark::Tpch(tpch_cfg.clone());
    pg_db.load_database(&tpch_benchmark).await?;
    if true {
        return Ok(());
    }
    let df_db = DatafusionDb::new().await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db), Box::new(df_db)];
    let cardtest_runner = CardtestRunner::new(databases).await?;
    cardtest_runner.load_databases(Benchmark::Test).await?;
    let qerrors = cardtest_runner.eval_qerrors("SELECT * FROM t1;").await?;
    println!("qerrors: {:?}", qerrors);
    let tpch_kit = TpchKit::build(true)?;
    tpch_kit.gen_queries(&tpch_cfg)?;
    Ok(())
}
