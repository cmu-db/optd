#[cfg(test)]
mod tests {
    use assert_cmd::prelude::CommandCargoExt;
    use optd_perftest::shell;
    use std::{
        fs,
        process::{Command, Stdio},
    };

    const WORKSPACE: &str = "optd_perftest_integration_workspace";

    /// Make sure Postgres is running before this test is run
    /// The reason I don't start Postgres automatically is because everyone has a different
    ///   preferred way of starting it (in Docker container, with Mac app, custom build, etc.)
    /// It's important to exercise all the different benchmarks to make sure their respective
    ///   kits, loading logic, and execution logic are sound.
    #[test_case::test_case("tpch")]
    // #[test_case::test_case("job")]
    fn cli_run_cardtest_twice(benchmark_name: &str) {
        // perform cleanup (clear workspace)
        let workspace_dpath = shell::parse_pathstr(WORKSPACE).unwrap();
        shell::make_into_empty_dir(&workspace_dpath).unwrap();

        // run command twice
        for i in 1..=2 {
            let mut cmd = create_cardtest_run_cmd(benchmark_name, false);
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

    fn create_cardtest_run_cmd(benchmark_name: &str, debug_print: bool) -> Command {
        let mut cmd = Command::cargo_bin("optd-perftest").unwrap();
        cmd.current_dir("..");
        cmd.args([
            "--workspace",
            WORKSPACE,
            "cardtest",
            "--benchmark-name",
            benchmark_name,
            // make sure scale factor is low so the test runs fast
            "--scale-factor",
            "0.01",
            "--pguser",
            "test_user",
            "--pgpassword",
            "password",
            // for all other args, whatever the default is is fine
        ]);
        if debug_print {
            cmd.env("RUST_LOG", "debug");
            cmd.env("RUST_BACKTRACE", "1");
            cmd.stdout(Stdio::inherit());
            cmd.stderr(Stdio::inherit());
        }
        cmd
    }
}
