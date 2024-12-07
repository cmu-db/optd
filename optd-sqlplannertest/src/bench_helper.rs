use std::{collections::VecDeque, future::Future, sync::Arc};

use crate::{extract_flags, DatafusionDBMS, TestFlags};
use anyhow::Result;
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan, sql::parser::Statement};
use tokio::runtime::Runtime;

pub trait PlannerBenchRunner {
    const BENCH_NAME: &str;
    type BenchInput;

    fn setup(
        &mut self,
        test_case: &sqlplannertest::ParsedTestCase,
    ) -> impl std::future::Future<Output = Result<(Self::BenchInput, TestFlags)>> + Send;

    fn bench(
        self,
        input: Self::BenchInput,
        test_case: &sqlplannertest::ParsedTestCase,
        flags: &TestFlags,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

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

pub struct PlanningBenchRunner(DatafusionDBMS);

impl PlanningBenchRunner {
    pub async fn new() -> Result<Self> {
        Ok(PlanningBenchRunner(DatafusionDBMS::new().await?))
    }
}

impl PlannerBenchRunner for PlanningBenchRunner {
    const BENCH_NAME: &str = "planning";
    type BenchInput = VecDeque<Statement>;
    async fn setup(
        &mut self,
        test_case: &sqlplannertest::ParsedTestCase,
    ) -> Result<(Self::BenchInput, TestFlags)> {
        for sql in &test_case.before_sql {
            self.0.execute(sql, &TestFlags::default()).await?;
        }
        let bench_task = test_case
            .tasks
            .iter()
            .find(|x| x.starts_with("bench"))
            .unwrap();
        let flags = extract_flags(bench_task)?;
        self.0.setup(&flags).await?;
        let statements = self.0.parse_sql(&test_case.sql).await?;

        Ok((statements, flags))
    }
    async fn bench(
        self,
        input: Self::BenchInput,
        _test_case: &sqlplannertest::ParsedTestCase,
        flags: &TestFlags,
    ) -> Result<()> {
        for stmt in input {
            self.0.create_physical_plan(stmt, flags).await?;
        }
        Ok(())
    }
}

pub struct ExecutionBenchRunner {
    pub dbms: DatafusionDBMS,
    pub populate_sql: String,
}

impl ExecutionBenchRunner {
    pub async fn new(populate_sql: String) -> Result<Self> {
        Ok(ExecutionBenchRunner {
            dbms: DatafusionDBMS::new().await?,
            populate_sql,
        })
    }
}

impl PlannerBenchRunner for ExecutionBenchRunner {
    const BENCH_NAME: &str = "execution";
    type BenchInput = Vec<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)>;
    async fn setup(
        &mut self,
        test_case: &sqlplannertest::ParsedTestCase,
    ) -> Result<(Self::BenchInput, TestFlags)> {
        for sql in &test_case.before_sql {
            self.dbms.execute(sql, &TestFlags::default()).await?;
        }
        for sql in self.populate_sql.split(";\n") {
            self.dbms.execute(sql, &TestFlags::default()).await?;
        }

        let bench_task = test_case
            .tasks
            .iter()
            .find(|x| x.starts_with("bench"))
            .unwrap();
        let flags = extract_flags(bench_task)?;

        self.dbms.setup(&flags).await?;
        let statements = self.dbms.parse_sql(&test_case.sql).await?;

        let mut physical_plans = Vec::new();
        for statement in statements {
            physical_plans.push(self.dbms.create_physical_plan(statement, &flags).await?);
        }

        Ok((physical_plans, flags))
    }
    async fn bench(
        self,
        input: Self::BenchInput,
        _test_case: &sqlplannertest::ParsedTestCase,
        _flags: &TestFlags,
    ) -> Result<()> {
        for (physical_plan, task_ctx) in input {
            self.dbms.execute_physical(physical_plan, task_ctx).await?;
        }
        Ok(())
    }
}
