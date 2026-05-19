use std::fs;
use std::path::{Path, PathBuf};

use datafusion_sqllogictest::{DFSqlLogicTestError, TestContext};
use optd_datafusion::runner::OptdRunner;
use optd_datafusion::setup::{setup_job_session, setup_tpch_session};
use sqllogictest::{
    Runner, default_column_validator, default_normalizer, default_validator, harness::Failed,
};
use tokio::runtime::Runtime;

fn main() {
    let (override_files, filters) = parse_override_args();
    let paths = collect_slt_paths(&filters);

    if override_files {
        for path in paths {
            update_slt(&path).unwrap_or_else(|err| panic!("failed to update {path:?}: {err:?}"));
        }
        return;
    }

    let mut tests = vec![];

    for path in paths {
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

fn parse_override_args() -> (bool, Vec<String>) {
    let mut override_files = false;
    let mut filters = Vec::new();
    for arg in std::env::args().skip(1) {
        if arg == "--override" {
            override_files = true;
        } else if override_files && !arg.starts_with("--") {
            filters.push(arg);
        }
    }
    (override_files, filters)
}

fn collect_slt_paths(filters: &[String]) -> Vec<PathBuf> {
    let paths =
        sqllogictest::harness::glob("tests/slt/**/*.slt").expect("failed to find test files");
    let mut paths = paths
        .map(|entry| entry.expect("failed to read glob entry"))
        .filter(|path| {
            filters.is_empty()
                || filters
                    .iter()
                    .any(|filter| path.to_string_lossy().contains(filter))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
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
        let session = if path.components().any(|c| c.as_os_str() == "tpch") {
            setup_tpch_session()
                .await
                .map_err(|e| Failed::from(e.to_string()))?
        } else if path.components().any(|c| c.as_os_str() == "job") {
            setup_job_session()
                .await
                .map_err(|e| Failed::from(e.to_string()))?
        } else {
            match TestContext::try_new_for_test_file(&path).await {
                Some(ctx) => ctx.session_ctx().clone(),
                None => datafusion::prelude::SessionContext::new(),
            }
        };

        let mut runner = Runner::new(|| async {
            Ok::<_, DFSqlLogicTestError>(OptdRunner::new(session.clone()))
        });
        runner.run_file_async(&path).await?;
        Ok(())
    })
}

fn update_slt(path: impl AsRef<Path>) -> Result<(), Failed> {
    let path = path.as_ref().to_path_buf();
    build_runtime().block_on(async {
        let session = if path.components().any(|c| c.as_os_str() == "tpch") {
            setup_tpch_session()
                .await
                .map_err(|e| Failed::from(e.to_string()))?
        } else if path.components().any(|c| c.as_os_str() == "job") {
            setup_job_session()
                .await
                .map_err(|e| Failed::from(e.to_string()))?
        } else {
            match TestContext::try_new_for_test_file(&path).await {
                Some(ctx) => ctx.session_ctx().clone(),
                None => datafusion::prelude::SessionContext::new(),
            }
        };

        let mut runner = Runner::new(|| async {
            Ok::<_, DFSqlLogicTestError>(OptdRunner::new(session.clone()))
        });
        runner
            .update_test_file(
                &path,
                "\t",
                default_validator,
                default_normalizer,
                default_column_validator,
            )
            .await
            .map_err(|e| Failed::from(e.to_string()))?;
        encode_blank_expected_rows(&path).map_err(|e| Failed::from(e.to_string()))?;
        Ok(())
    })
}

fn encode_blank_expected_rows(path: &Path) -> std::io::Result<()> {
    let text = fs::read_to_string(path)?;
    let lines = text.lines().collect::<Vec<_>>();
    let mut rewritten = Vec::with_capacity(lines.len());
    let mut in_results = false;

    for (idx, line) in lines.iter().enumerate() {
        if *line == "----" {
            in_results = true;
            rewritten.push((*line).to_string());
            continue;
        }

        if in_results && line.is_empty() {
            let next = lines.get(idx + 1).copied().unwrap_or_default();
            if !next.is_empty() && !is_record_start(next) {
                rewritten.push(" ".to_string());
                continue;
            }
            in_results = false;
        }

        rewritten.push((*line).to_string());
    }

    fs::write(path, format!("{}\n", rewritten.join("\n")))
}

fn is_record_start(line: &str) -> bool {
    let first = line.split_whitespace().next();
    matches!(
        first,
        Some(
            "connection"
                | "control"
                | "halt"
                | "hash-threshold"
                | "include"
                | "onlyif"
                | "query"
                | "require"
                | "skipif"
                | "statement"
                | "subtest"
                | "system"
        )
    )
}
