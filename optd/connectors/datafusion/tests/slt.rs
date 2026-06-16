use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use datafusion_sqllogictest::{DFSqlLogicTestError, TestContext};
use optd_datafusion::runner::OptdRunner;
use optd_datafusion::setup::{setup_job_session, setup_tpch_session};
#[cfg(not(feature = "duckdb"))]
use optd_datafusion::slt_reference::duckdb_feature_error;
use optd_datafusion::slt_reference::{DataFusionRunner, ExpectedEngine, parse_slt_args};
use sqllogictest::{
    Runner, default_column_validator, default_normalizer, default_validator, harness::Failed,
};
use tokio::runtime::Runtime;

const DISABLED_SLT_PATHS: &[&str] = &[
    // Deferred: these JOB result queries currently exceed the nextest release
    // timeout in the full workspace run. Re-enable once the execution path is
    // fast enough or these cases get a dedicated long-running test profile.
    "tests/slt/job/results/16a.slt",
    "tests/slt/job/results/16b.slt",
    "tests/slt/job/results/16c.slt",
    "tests/slt/job/results/16d.slt",
    "tests/slt/job/results/19d.slt",
];

fn main() {
    let args = parse_slt_args(std::env::args().skip(1)).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(2);
    });
    let paths = collect_slt_paths(&args.filters);

    if args.override_files {
        let total = paths.len();
        for (index, path) in paths.iter().enumerate() {
            let started = Instant::now();
            eprintln!(
                "START [{}/{}] {} ({})",
                index + 1,
                total,
                path.display(),
                args.expected_engine
            );
            match update_slt(path, args.expected_engine) {
                Ok(()) => {
                    eprintln!(
                        "PASS  [{}/{}] {} ({:.3}s)",
                        index + 1,
                        total,
                        path.display(),
                        started.elapsed().as_secs_f64(),
                    );
                }
                Err(err) => {
                    eprintln!(
                        "FAIL  [{}/{}] {} ({:.3}s)",
                        index + 1,
                        total,
                        path.display(),
                        started.elapsed().as_secs_f64(),
                    );
                    panic!("failed to update {path:?}: {err:?}");
                }
            }
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

fn collect_slt_paths(filters: &[String]) -> Vec<PathBuf> {
    let paths =
        sqllogictest::harness::glob("tests/slt/**/*.slt").expect("failed to find test files");
    let mut paths = paths
        .map(|entry| entry.expect("failed to read glob entry"))
        .filter(|path| {
            let normalized = path.to_string_lossy().replace('\\', "/");
            if DISABLED_SLT_PATHS
                .iter()
                .any(|disabled| normalized.ends_with(disabled))
            {
                return false;
            }
            filters.is_empty() || filters.iter().any(|filter| normalized.contains(filter))
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
    let runtime = build_runtime();
    let result = runtime.block_on(async {
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
    });
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}

fn update_slt(path: impl AsRef<Path>, expected_engine: ExpectedEngine) -> Result<(), Failed> {
    let path = path.as_ref().to_path_buf();
    let runtime = build_runtime();
    let result = runtime.block_on(async {
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

        match expected_engine {
            ExpectedEngine::OptdDataFusion => {
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
            }
            ExpectedEngine::DataFusion => {
                let mut runner = Runner::new(|| async {
                    Ok::<_, DFSqlLogicTestError>(DataFusionRunner::new(session.clone()))
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
            }
            ExpectedEngine::DuckDb => {
                update_slt_with_duckdb(&path).await?;
            }
        }
        encode_blank_expected_rows(&path).map_err(|e| Failed::from(e.to_string()))?;
        Ok(())
    });
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}

#[cfg(feature = "duckdb")]
async fn update_slt_with_duckdb(path: &Path) -> Result<(), Failed> {
    use optd_datafusion::slt_reference::DuckDbRunner;

    let path = path.to_path_buf();
    let mut runner = Runner::new(|| {
        let path = path.clone();
        async move { DuckDbRunner::new_for_path(&path) }
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
        .map_err(|e| Failed::from(e.to_string()))
}

#[cfg(not(feature = "duckdb"))]
async fn update_slt_with_duckdb(_path: &Path) -> Result<(), Failed> {
    Err(Failed::from(duckdb_feature_error().to_string()))
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
