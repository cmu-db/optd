// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::path::Path;

use anyhow::Result;
use clap::Parser;
use sqlplannertest::PlannerTestApplyOptions;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional list of test modules or test files to apply the test; if empty, apply all tests
    selections: Vec<String>,
    /// Use the advanced cost model
    #[clap(long)]
    enable_advanced_cost_model: bool,
    /// Execute tests in serial
    #[clap(long)]
    serial: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, EnvFilter};

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    unsafe { backtrace_on_stack_overflow::enable() };

    let cli = Cli::parse();

    let enable_advanced_cost_model = cli.enable_advanced_cost_model;
    let opts = PlannerTestApplyOptions {
        serial: cli.serial,
        selections: cli.selections,
    };

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

    Ok(())
}
