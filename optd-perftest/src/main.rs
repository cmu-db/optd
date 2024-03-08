use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use anyhow::Result;
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;
use std::path::Path;

use crate::{cardtest::Benchmark, tpch::{TpchConfig, TpchKit}};

mod cardtest;
mod datafusion_db_cardtest;
mod postgres_db;
mod tpch;
mod cmd;

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
    let kit = TpchKit::build(TpchConfig {scale_factor: 1}).unwrap();
    kit.gen_tpch_tables();
    println!("file!(): {:?}", Path::new(file!()).parent().unwrap().to_path_buf());
    Ok(())
}
