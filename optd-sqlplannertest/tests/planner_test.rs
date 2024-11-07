// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::path::Path;

use anyhow::Result;

fn main() -> Result<()> {
    sqlplannertest::planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"),
        || async { optd_sqlplannertest::DatafusionDBMS::new().await },
    )?;
    Ok(())
}
