use std::path::Path;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    sqlplannertest::planner_test_apply(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        || async { optd_sqlplannertest::DatafusionDBMS::new().await },
    )
    .await?;
    Ok(())
}
