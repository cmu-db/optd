use anyhow::Result;
use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;

use crate::{
    cardtest::Benchmark,
    tpch_kit::{TpchKit, TPCH_KIT_POSTGRES},
};

mod cardtest;
mod cmd;
mod datafusion_db_cardtest;
mod postgres_db;
mod tpch_kit;

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
    let kit = TpchKit::build(true).unwrap();
    kit.gen_tables(TPCH_KIT_POSTGRES, 1)?;
    kit.gen_queries(TPCH_KIT_POSTGRES, 1, 15721)?;
    Ok(())
}
