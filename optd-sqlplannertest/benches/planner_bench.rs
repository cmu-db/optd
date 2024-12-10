use std::{
    future::Future,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use optd_sqlplannertest::bench_helper::{
    bench_run, bench_setup, ExecutionBenchRunner, PlannerBenchRunner, PlanningBenchRunner,
};
use sqlplannertest::{discover_tests_with_selections, parse_test_cases, TestCase};
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let selection = "tpch";
    let selections = vec![selection.to_string()];

    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    planner_bench_runner(
        &tests_dir,
        || async { PlanningBenchRunner::new().await },
        &selections,
        c,
    )
    .unwrap();

    let path = tests_dir.join(format!("{selection}/bench_populate.sql"));
    let populate_sql = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))
        .unwrap();

    planner_bench_runner(
        &tests_dir,
        move || {
            let populate_sql = populate_sql.clone();
            async { ExecutionBenchRunner::new(populate_sql).await }
        },
        &selections,
        c,
    )
    .unwrap();
}

/// Discovers and bench each test case.
///
/// The user needs to provide a runner function that creates a runner that
/// implements the [`PlannerBenchRunner`] trait.
///
/// A test case will be selected if:
///
/// 1. it's included in the `tests_dir` as part of `selections`.
/// 2. has `bench` listed in the task list.
///
/// ## Limitation
///
/// Currently only accept sqlplannertest files with single test case.
fn planner_bench_runner<F, Ft, R>(
    tests_dir: impl AsRef<Path>,
    runner_fn: F,
    selections: &[String],
    c: &mut Criterion,
) -> Result<()>
where
    F: Fn() -> Ft + Send + Sync + 'static + Clone,
    Ft: Future<Output = Result<R>> + Send,
    R: PlannerBenchRunner + 'static,
{
    let tests = discover_tests_with_selections(&tests_dir, selections)?
        .map(|path| {
            let path = path?;
            let relative_path = path
                .strip_prefix(&tests_dir)
                .context("unable to relative path")?
                .as_os_str();
            let testname = relative_path
                .to_str()
                .context("unable to convert to string")?
                .to_string();
            Ok::<_, anyhow::Error>((path, testname))
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (path, testname) in tests {
        bench_runner(path, testname, runner_fn.clone(), c)?;
    }

    Ok(())
}

/// Bench runner for a test case.
fn bench_runner<F, Ft, R>(
    path: PathBuf,
    testname: String,
    runner_fn: F,
    c: &mut Criterion,
) -> Result<()>
where
    F: Fn() -> Ft + Send + Sync + 'static + Clone,
    Ft: Future<Output = Result<R>> + Send,
    R: PlannerBenchRunner,
{
    fn build_runtime() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    let testcases = std::fs::read(&path)?;
    let testcases: Vec<TestCase> = serde_yaml::from_slice(&testcases)?;

    let testcases = parse_test_cases(
        {
            let mut path = path.clone();
            path.pop();
            path
        },
        testcases,
    )?;

    if testcases.len() != 1 {
        bail!(
            "planner_bench can only handle sqlplannertest yml file with one test cases, {} has {}",
            path.display(),
            testcases.len()
        );
    }

    let testcase = &testcases[0];

    let should_bench = testcase.tasks.iter().any(|x| x.starts_with("bench"));
    if should_bench {
        let mut group = c.benchmark_group(testname.strip_suffix(".yml").unwrap());
        let runtime = build_runtime();
        group.bench_function(R::BENCH_NAME, |b| {
            b.iter_batched(
                || bench_setup(&runtime, runner_fn.clone(), testcase),
                |(runner, input, flags)| {
                    bench_run(&runtime, runner, black_box(input), testcase, &flags)
                },
                BatchSize::PerIteration,
            );
        });
        group.finish();
    }
    Ok(())
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
