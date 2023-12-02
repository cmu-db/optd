use std::path::Path;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    sqlplannertest::planner_test_apply(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        || async { Ok(optd_sqlplannertest::DatafusionDb::new().await?) },
    )
    .await?;
    Ok(())
}
