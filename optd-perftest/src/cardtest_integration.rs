#[cfg(test)]
mod tests {
    use std::{fs, process::Command};
    use assert_cmd::prelude::CommandCargoExt;
    use crate::shell;

    const WORKSPACE: &str = "../optd_perftest_integration_workspace";

    /// We run it twice to test clean slate behavior and behavior with a cache
    /// Note that Postgres must _not_ be running for this test to run properly.
    /// It is difficult (and possibly destructive) to programmatically close Postgres,
    ///   so I chose not to do it. The user must do this manually.
    #[test]
    fn cli_run_cardtest_twice() {
        // perform cleanup (clear workspace)
        let workspace_dpath = shell::parse_pathstr(WORKSPACE).unwrap();
        shell::make_into_empty_dir(&workspace_dpath).unwrap();

        // run command twice
        for i in 1..=2 {
            let mut cmd = create_cardtest_run_cmd();
            let output = cmd.output().unwrap();
            assert!(
                output.status.success(),
                "cardtest run #{} failed with ```{}```",
                i,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // delete workspace
        fs::remove_dir_all(&workspace_dpath).unwrap();
    }

    fn create_cardtest_run_cmd() -> Command {
        let mut cmd = Command::cargo_bin("optd-perftest").unwrap();
        cmd.current_dir(".."); // all paths in `test.sql` assume we're in the base dir of the repo
        cmd.args([
            "--workspace",
            WORKSPACE,
            "cardtest",
            "--scale-factor",
            "0.01",
            "--seed",
            "15721",
        ]);
        cmd
    }
}