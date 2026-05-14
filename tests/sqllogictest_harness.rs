use std::path::Path;

use sqllogictest::{Runner, harness::Failed};
use tokio::runtime::Runtime;

mod common;

use common::sqllogictest_support::DataFusionSubstraitDb;

fn main() {
    let mut tests = Vec::new();

    collect_sqllogictests(&mut tests, "tests/slt/*.slt");

    if std::env::var("INCLUDE_TPCH").as_deref() == Ok("true") {
        collect_sqllogictests(&mut tests, "tests/slt/tpch/*.slt");
    }

    if tests.is_empty() {
        panic!("no sqllogictest files found");
    }

    sqllogictest::harness::run(&sqllogictest::harness::Arguments::from_args(), tests).exit();
}

fn collect_sqllogictests(tests: &mut Vec<sqllogictest::harness::Trial>, pattern: &str) {
    let paths = sqllogictest::harness::glob(pattern).expect("failed to find test files");

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        tests.push(sqllogictest::harness::Trial::test(
            path.to_str().unwrap().to_string(),
            move || run_sqllogictest(&path),
        ));
    }
}

fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn run_sqllogictest(filename: impl AsRef<Path>) -> Result<(), Failed> {
    build_runtime().block_on(async {
        let mut runner = Runner::new(|| async { DataFusionSubstraitDb::new() });
        runner.run_file_async(filename).await?;
        Ok(())
    })
}
