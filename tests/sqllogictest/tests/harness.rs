use std::path::Path;

use optd_datafusion::DataFusionDB;
use optd_sqllogictest::DBWrapper;
use sqllogictest::{Runner, harness::Failed};
use tokio::runtime::Runtime;

fn main() {
    let paths = sqllogictest::harness::glob("slt/**/*.slt").expect("failed to find test files");
    let mut tests = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        tests.push(sqllogictest::harness::Trial::test(
            path.to_str().unwrap().to_string(),
            move || test(&path),
        ));
    }

    if tests.is_empty() {
        panic!("no test found for sqllogictest under: slt/**/*.slt");
    }
    sqllogictest::harness::run(&sqllogictest::harness::Arguments::from_args(), tests).exit();
}

fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn test(filename: impl AsRef<Path>) -> Result<(), Failed> {
    build_runtime().block_on(async {
        let mut tester = Runner::new(|| async { Ok(DBWrapper(DataFusionDB::new().await?)) });
        tester.run_file_async(filename).await?;
        Ok(())
    })
}
