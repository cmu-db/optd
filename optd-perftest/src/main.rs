use anyhow::Result;
use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;

use crate::{
    cardtest::Benchmark,
    tpch_kit::{TpchKit, TPCH_KIT_POSTGRES},
};

mod cardtest;
mod shell;
mod datafusion_db_cardtest;
mod postgres_db;
mod tpch_kit;

#[tokio::main]
async fn main() -> Result<()> {
    let pg_db = PostgresDb::build(true).await?;
    pg_db.load_benchmark_data().await?;
    if true {
        return Ok(());
    }
    let df_db = DatafusionDb::new().await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![
        Box::new(pg_db),
        Box::new(df_db),
    ];
    let cardtest_runner = CardtestRunner::new(databases).await?;
    cardtest_runner.load_databases(Benchmark::Test).await?;
    let qerrors = cardtest_runner.eval_qerrors("SELECT * FROM t1;").await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
