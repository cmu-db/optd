use std::path::Path;

use datafusion::prelude::SessionContext;
use simple_graph_datafusion::runner::SimpleGraphRunner;
use sqllogictest::Runner;

#[tokio::test]
async fn run_basic_slt() {
    let slt_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt/basic.slt");
    let session = SessionContext::new();
    let mut runner = Runner::new(|| async {
        Ok::<_, datafusion_sqllogictest::DFSqlLogicTestError>(SimpleGraphRunner::new(
            session.clone(),
        ))
    });
    runner.run_file_async(slt_path).await.unwrap();
}
