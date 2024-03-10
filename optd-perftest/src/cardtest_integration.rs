#[cfg(test)]
mod tests {
    use std::{fs, process::Command};
    use assert_cmd::prelude::CommandCargoExt;
    use crate::shell;

    const WORKSPACE: &str = "../optd_perftest_integration_workspace";

    #[test]
    fn cli_test_tpch() {
        // make sure workspace is empty
        let workspace_dpath = shell::parse_pathstr(WORKSPACE).unwrap();
        shell::make_into_empty_dir(&workspace_dpath).unwrap();

        // run command
        let mut cmd = Command::cargo_bin("optd-perftest").unwrap();
        cmd.current_dir(".."); // all paths in `test.sql` assume we're in the base dir of the repo
        cmd.args([
            "--workspace",
            WORKSPACE,
            "cardtest",
        ]);
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "should have ran cardtest successfully. it failed with ```{}```",
            String::from_utf8_lossy(&output.stderr)
        );

        // delete workspace
        fs::remove_dir_all(&workspace_dpath).unwrap();
    }
}
