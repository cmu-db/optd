use std::path::Path;

use anyhow::Result;
use clap::Parser;
use sqlplannertest::PlannerTestApplyOptions;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional list of directories to apply the test; if empty, apply all tests
    directories: Vec<String>,
    /// Use the advanced cost model
    #[clap(long)]
    enable_advanced_cost_model: bool,
    /// Execute tests in serial
    #[clap(long)]
    serial: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let enable_advanced_cost_model = cli.enable_advanced_cost_model;
    let opts = PlannerTestApplyOptions { serial: cli.serial };

    if cli.directories.is_empty() {
        println!("Running all tests");
        sqlplannertest::planner_test_apply_with_options(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
            move || async move {
                if enable_advanced_cost_model {
                    optd_sqlplannertest::DatafusionDBMS::new_advanced_cost().await
                } else {
                    optd_sqlplannertest::DatafusionDBMS::new().await
                }
            },
            opts,
        )
        .await?;
    } else {
        for directory in cli.directories {
            println!("Running tests in {}", directory);
            sqlplannertest::planner_test_apply_with_options(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join(directory),
                move || async move {
                    if enable_advanced_cost_model {
                        optd_sqlplannertest::DatafusionDBMS::new_advanced_cost().await
                    } else {
                        optd_sqlplannertest::DatafusionDBMS::new().await
                    }
                },
                opts.clone(),
            )
            .await?;
        }
    }
    Ok(())
}
