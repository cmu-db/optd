use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::{DFSqlLogicTestError, TestContext};
use optd_datafusion::config::OptdExtensionConfig;
use optd_datafusion::runner::OptdRunner;
use optd_datafusion::setup::{setup_job_session, setup_tpch_session};
#[cfg(not(feature = "duckdb"))]
use optd_datafusion::slt_reference::duckdb_feature_error;
use optd_datafusion::slt_reference::{
    DataFusionRunner, ExpectedEngine, parse_slt_args, strip_slt_args_for_harness,
};
use sqllogictest::{
    Runner, default_column_validator, default_normalizer, default_validator, harness::Failed,
};
use tokio::runtime::Runtime;

const DISABLED_SLT_PATHS: &[&str] = &[
    // Deferred: this JOB result query still runs too long under the default
    // physical-planning path. Re-enable once the execution path is faster.
    "tests/slt/job/results/19d.slt",
];

fn main() {
    let raw_args = std::env::args().collect::<Vec<_>>();
    let args = parse_slt_args(raw_args.iter().skip(1).cloned()).unwrap_or_else(|err| {
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
            move || run_slt(&path, args.physical_planning),
        ));
    }

    if tests.is_empty() {
        panic!("no test files found under tests/slt/**/*.slt");
    }

    let harness_args = strip_slt_args_for_harness(raw_args);
    sqllogictest::harness::run(
        &sqllogictest::harness::Arguments::from_iter(harness_args),
        tests,
    )
    .exit();
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

fn run_slt(path: impl AsRef<Path>, physical_planning: bool) -> Result<(), Failed> {
    let path = path.as_ref().to_path_buf();
    let runtime = build_runtime();
    let result = runtime.block_on(async {
        let (session, _test_context) = session_for_slt_file(&path).await?;
        if !physical_planning {
            session
                .sql("SET optd.physical_planning = false")
                .await
                .map_err(|e| Failed::from(e.to_string()))?
                .collect()
                .await
                .map_err(|e| Failed::from(e.to_string()))?;
        }

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
        let (session, _test_context) = session_for_slt_file(&path).await?;

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

async fn session_for_slt_file(
    path: &Path,
) -> Result<(SessionContext, Option<TestContext>), Failed> {
    let session = if path.components().any(|c| c.as_os_str() == "tpch") {
        setup_tpch_session()
            .await
            .map_err(|e| Failed::from(e.to_string()))?
    } else if path.components().any(|c| c.as_os_str() == "job") {
        setup_job_session()
            .await
            .map_err(|e| Failed::from(e.to_string()))?
    } else {
        let Some(test_context) = TestContext::try_new_for_test_file(path).await else {
            return Err(Failed::from(format!(
                "DataFusion TestContext skipped or could not initialize {}",
                path.display()
            )));
        };
        let session = test_context.session_ctx().clone();
        ensure_optd_extension(&session);
        return Ok((session, Some(test_context)));
    };
    ensure_optd_extension(&session);
    Ok((session, None))
}

fn ensure_optd_extension(session: &SessionContext) {
    let state = session.state_ref();
    let mut state = state.write();
    let extensions = &mut state.config_mut().options_mut().extensions;
    if extensions.get::<OptdExtensionConfig>().is_none() {
        extensions.insert(OptdExtensionConfig::default());
    }
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
