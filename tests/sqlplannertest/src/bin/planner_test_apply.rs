// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::path::Path;

use anyhow::Result;
use clap::Parser;
use optd_sqlplannertest::PlannerTestDB;
use sqlplannertest::PlannerTestApplyOptions;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional list of test modules or test files to apply the test; if empty, apply all tests
    selections: Vec<String>,
    /// Use the advanced cost model
    #[clap(long)]
    /// Execute tests in serial
    #[clap(long)]
    serial: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let opts = PlannerTestApplyOptions {
        serial: cli.serial,
        selections: cli.selections,
    };

    sqlplannertest::planner_test_apply_with_options(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        move || async move { Ok(PlannerTestDB(optd_datafusion::DataFusionDB::new().await?)) },
        opts,
    )
    .await?;

    Ok(())
}
