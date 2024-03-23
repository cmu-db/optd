use std::path::Path;

use anyhow::Result;

fn main() -> Result<()> {
    sqlplannertest::planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        || async { optd_sqlplannertest::DatafusionDBMS::new().await },
    )?;
    Ok(())
}
