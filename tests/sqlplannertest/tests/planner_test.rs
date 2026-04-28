use std::path::Path;

use anyhow::Result;
use optd_sqlplannertest::PlannerTestDB;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    sqlplannertest::planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        || async { Ok(PlannerTestDB(optd_datafusion::DataFusionDB::new().await?)) },
    )?;
    Ok(())
}
