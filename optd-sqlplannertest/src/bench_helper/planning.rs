use std::collections::VecDeque;

use crate::{extract_flags, DatafusionDBMS, TestFlags};
use anyhow::Result;
use datafusion::sql::parser::Statement;

use super::PlannerBenchRunner;

/// A benchmark runner for evaluating optimizer planning time.
pub struct PlanningBenchRunner(DatafusionDBMS);

impl PlanningBenchRunner {
    pub async fn new() -> Result<Self> {
        Ok(PlanningBenchRunner(DatafusionDBMS::new().await?))
    }
}

/// With parsed statements as input,
/// measures the time it takes to generate datafusion physical plans.
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
