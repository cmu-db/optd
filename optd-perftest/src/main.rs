use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use anyhow::Result;
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;

use crate::cardtest::Benchmark;

mod cardtest;
mod datafusion_db_cardtest;
mod postgres_db;

#[tokio::main]
async fn main() -> Result<()> {
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![
        Box::new(PostgresDb::new().await?),
        Box::new(DatafusionDb::new().await?),
    ];
    let cardtest_runner = CardtestRunner::new(databases).await?;
    cardtest_runner.load_databases(Benchmark::Test).await?;
    let qerrors = cardtest_runner.eval_qerrors("SELECT * FROM t1;").await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
