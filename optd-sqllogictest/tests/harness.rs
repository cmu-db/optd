// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::path::Path;

use optd_sqllogictest::DatafusionDBMS;
use sqllogictest::{harness::Failed, Runner};
use tokio::runtime::Runtime;

// TODO: sqllogictest harness should support async new function

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
        // let mut tester = Runner::new(|| async { Ok(DatafusionDBMS::new_no_optd().await?) });
        let mut tester = Runner::new(|| async { DatafusionDBMS::new().await });
        tester.run_file_async(filename).await?;
        Ok(())
    })
}
