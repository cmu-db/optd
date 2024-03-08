use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use anyhow::Result;
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;

use crate::cardtest::Benchmark;

mod cardtest;
<<<<<<< HEAD
mod datafusion_db_cardtest;
mod postgres_db;
=======
mod postgres;
mod tpch;
>>>>>>> 1eb364c (moved tpch/ into cli/. updated readme)

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
