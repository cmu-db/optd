use std::path::Path;

use anyhow::Result;

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional list of directories to apply the test; if empty, apply all tests
    directories: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.directories.is_empty() {
        println!("Running all tests");
        sqlplannertest::planner_test_apply(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
            || async { optd_sqlplannertest::DatafusionDBMS::new().await },
        )
        .await?;
    } else {
        for directory in cli.directories {
            println!("Running tests in {}", directory);
            sqlplannertest::planner_test_apply(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join(directory),
                || async { optd_sqlplannertest::DatafusionDBMS::new().await },
            )
            .await?;
        }
    }
    Ok(())
}
