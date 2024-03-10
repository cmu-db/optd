use std::process::Command;
use assert_cmd::prelude::CommandCargoExt;

#[test]
fn cli_test_tpch() {
    let mut cmd = Command::cargo_bin("optd-perftest").unwrap();
    cmd.current_dir(".."); // all paths in `test.sql` assume we're in the base dir of the repo
    cmd.args([
        "--enable-logical",
        "--file",
        "datafusion-optd-cli/tpch-sf0_01/test.sql",
    ]);
    let status = cmd.status().unwrap();
    assert!(
        status.success(),
        "should not have crashed when running tpch"
    );
}