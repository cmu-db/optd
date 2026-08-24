use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use optd_statsregtest::{RunConfig, apply_baselines, run_against_baselines};

#[derive(Debug, Parser)]
#[command(version, about = "Statistics regression harness for optd")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long, default_value = "tpch")]
        suite: String,
        selections: Vec<String>,
    },
    Apply {
        #[arg(long, default_value = "tpch")]
        suite: String,
        selections: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    match cli.command {
        Command::Run { suite, selections } => {
            let report =
                run_against_baselines(&RunConfig::new(workspace_root, suite, selections)).await?;
            println!("{}", report.markdown);
        }
        Command::Apply { suite, selections } => {
            let summary =
                apply_baselines(&RunConfig::new(workspace_root, suite, selections)).await?;
            for path in summary.written {
                println!("{}", path.display());
            }
        }
    }

    Ok(())
}
