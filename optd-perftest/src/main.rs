use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use clap::{Parser, Subcommand};
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

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Cardtest {
        #[arg(long)]
        #[clap(default_value = "0.01")]
        scale_factor: f64,
        #[arg(long)]
        #[clap(default_value = "15721")]
        seed: i32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Cardtest { scale_factor, seed } => {
            let tpch_config = TpchConfig {
                database: String::from(TPCH_KIT_POSTGRES),
                scale_factor: *scale_factor,
                seed: *seed,
            };
            cardtest(tpch_config).await
        }
    }
}

async fn cardtest(tpch_config: TpchConfig) -> anyhow::Result<()> {
    let pg_db = PostgresDb::build().await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db)];

    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    let mut cardtest_runner = CardtestRunner::new(databases).await?;
    let qerrors = cardtest_runner
        .eval_benchmark_qerrors_alldbs(&tpch_benchmark)
        .await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
