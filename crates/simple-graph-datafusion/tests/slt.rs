use std::path::Path;

use datafusion_sqllogictest::{DFSqlLogicTestError, TestContext};
use simple_graph_datafusion::runner::SimpleGraphRunner;
use sqllogictest::{Runner, harness::Failed};
use tokio::runtime::Runtime;

fn main() {
    let paths =
        sqllogictest::harness::glob("tests/slt/**/*.slt").expect("failed to find test files");
    let mut tests = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        tests.push(sqllogictest::harness::Trial::test(
            path.to_str().unwrap().to_string(),
            move || run_slt(&path),
        ));
    }

    if tests.is_empty() {
        panic!("no test files found under tests/slt/**/*.slt");
    }

    sqllogictest::harness::run(&sqllogictest::harness::Arguments::from_args(), tests).exit();
}

fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn run_slt(path: impl AsRef<Path>) -> Result<(), Failed> {
    let path = path.as_ref().to_path_buf();
    build_runtime().block_on(async {
        let test_ctx = TestContext::try_new_for_test_file(&path)
            .await
            .unwrap_or_else(|| TestContext::new(datafusion::prelude::SessionContext::new()));
        let session = test_ctx.session_ctx().clone();
        let mut runner = Runner::new(|| async {
            Ok::<_, DFSqlLogicTestError>(SimpleGraphRunner::new(session.clone()))
        });
        runner.run_file_async(&path).await?;
        Ok(())
    })
}
