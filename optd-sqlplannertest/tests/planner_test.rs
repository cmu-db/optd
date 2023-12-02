use std::path::Path;

use anyhow::Result;

fn main() -> Result<()> {
    sqlplannertest::planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        || async { Ok(optd_sqlplannertest::DatafusionDb::new().await?) },
    )?;
    Ok(())
}
