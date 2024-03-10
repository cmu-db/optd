use std::{fs, path::{Path, PathBuf}};

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
    #[arg(long)]
    #[clap(default_value = "../optd_perftest_workspace")]
    #[clap(help = "The directory where artifacts required for performance testing (such as pgdata or TPC-H queries) are generated. Can be an absolute path or a relative path. Regardless of where this CLI is run, relative paths are evaluated relative to the optd repo root.")]
    workspace: String,
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
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let workspace = PathBuf::from(cli.workspace);
    let workspace = if workspace.is_relative() {
        shell::get_optd_root()?.join(workspace)
    } else {
        workspace
    };
    if !workspace.exists() {
        fs::create_dir(&workspace)?;
    }

    match &cli.command {
        Commands::Cardtest { scale_factor, seed } => {
            let tpch_config = TpchConfig {
                database: String::from(TPCH_KIT_POSTGRES),
                scale_factor: *scale_factor,
                seed: *seed,
            };
            cardtest(&workspace, tpch_config).await
        }
    }
}

async fn cardtest<P>(workspace: P, tpch_config: TpchConfig) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let pg_db = PostgresDb::build(true).await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db)];
    
    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    let cardtest_runner = CardtestRunner::new(workspace, databases).await?;
    let qerrors = cardtest_runner
        .eval_benchmark_qerrors_alldbs(&tpch_benchmark)
        .await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}
