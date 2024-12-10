pub mod execution;
pub mod planning;

use std::future::Future;

use crate::TestFlags;
use anyhow::Result;
use tokio::runtime::Runtime;

pub use execution::ExecutionBenchRunner;
pub use planning::PlanningBenchRunner;

pub trait PlannerBenchRunner {
    /// Describes what the benchmark is evaluating.
    const BENCH_NAME: &str;
    /// Benchmark's input.
    type BenchInput;

    /// Setups the necessary environment for the benchmark based on the test case.
    /// Returns the input needed for the benchmark.
    fn setup(
        &mut self,
        test_case: &sqlplannertest::ParsedTestCase,
    ) -> impl std::future::Future<Output = Result<(Self::BenchInput, TestFlags)>> + Send;

    /// Runs the actual benchmark based on the test case and input.
    fn bench(
        self,
        input: Self::BenchInput,
        test_case: &sqlplannertest::ParsedTestCase,
        flags: &TestFlags,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Sync wrapper for [`PlannerBenchRunner::setup`]
pub fn bench_setup<F, Ft, R>(
    runtime: &Runtime,
    runner_fn: F,
    testcase: &sqlplannertest::ParsedTestCase,
) -> (R, R::BenchInput, TestFlags)
where
    F: Fn() -> Ft + Send + Sync + 'static + Clone,
    Ft: Future<Output = Result<R>> + Send,
    R: PlannerBenchRunner,
{
    runtime.block_on(async {
        let mut runner = runner_fn().await.unwrap();
        let (input, flags) = runner.setup(testcase).await.unwrap();
        (runner, input, flags)
    })
}

/// Sync wrapper for [`PlannerBenchRunner::bench`]
pub fn bench_run<R>(
    runtime: &Runtime,
    runner: R,
    input: R::BenchInput,
    testcase: &sqlplannertest::ParsedTestCase,
    flags: &TestFlags,
) where
    R: PlannerBenchRunner,
{
    runtime.block_on(async { runner.bench(input, testcase, flags).await.unwrap() });
}
