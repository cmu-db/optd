// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::{Command, Stdio};

    use assert_cmd::prelude::CommandCargoExt;
    use optd_perfbench::shell;

    const WORKSPACE: &str = "optd_perfbench_integration_workspace";

    /// Make sure Postgres is running before this test is run
    /// The reason I don't start Postgres automatically is because everyone has a different
    ///   preferred way of starting it (in Docker container, with Mac app, custom build, etc.)
    /// It's important to exercise all the different benchmarks to make sure their respective
    ///   kits, loading logic, and execution logic are sound.
    /// While it'd be nice to test JOB, JOB only has one scale factor and that scale factor
    ///   takes 30 minutes to build stats as of 4/15/24, so we don't test it right now.
    #[test_case::test_case("tpch")]
    fn cli_run_cardbench_twice(benchmark_name: &str) {
        // perform cleanup (clear workspace)
        let workspace_dpath = shell::parse_pathstr(WORKSPACE).unwrap();
        shell::make_into_empty_dir(&workspace_dpath).unwrap();

        // run command twice
        for i in 1..=2 {
            let mut cmd = create_cardbench_run_cmd(benchmark_name, false);
            let output = cmd.output().unwrap();
            assert!(
                output.status.success(),
                "cardbench run #{} failed with ```{}```",
                i,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // delete workspace
        fs::remove_dir_all(&workspace_dpath).unwrap();
    }

    fn create_cardbench_run_cmd(benchmark_name: &str, debug_print: bool) -> Command {
        let mut cmd = Command::cargo_bin("optd-perfbench").unwrap();
        cmd.current_dir("..");
        cmd.args([
            "--workspace",
            WORKSPACE,
            "cardbench",
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
