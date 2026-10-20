//! Local query performance measurements using the production optimizer and direct physical path.
//!
//! Statistics/import is included in total planning, but reported separately. A harness caches runtime
//! aggregate statistics across warm-up and measured runs. No logical/DataFusion fallback is allowed.

use std::path::Path;
use std::time::Instant;

use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::DFColumnType;
use serde::{Deserialize, Serialize};
use sqllogictest::{Connection, QueryExpect, Record};

use crate::runner::{
    TryViaIrError, build_physical_ir, default_pass_manager, optimizer_context_for_benchmark,
    run_optimizer,
};
use crate::runtime_statistics::RuntimeStatisticsCatalogBuilder;
use crate::to_df_physical::ToPhysicalError;

/// One invocation of an optimizer pass, outside any full snapshot-tracing mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkPass {
    pub iteration: usize,
    pub pass_index: usize,
    pub pass: String,
    pub changed: Option<bool>,
    pub duration_ms: f64,
}

/// A successful fresh planning/execution sample. Times use monotonic wall-clock milliseconds.
///
/// Execution buffers and fully consumes results, just like the production direct runner. Counting
/// output rows and formatting the final physical plan are outside the timed regions. Row counts are
/// diagnostics, not correctness verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSample {
    pub sql_planning_ms: f64,
    pub statistics_import_ms: f64,
    pub optimizer_ms: f64,
    pub physical_planning_ms: f64,
    pub planning_ms: f64,
    pub execution_ms: f64,
    pub output_rows: usize,
    pub passes: Vec<BenchmarkPass>,
    pub physical_plan: String,
}

/// A failure that must never be converted into a successful timing sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkError {
    /// `unsupported` for a known unsupported path, otherwise `error`.
    pub status: String,
    pub message: String,
}

impl BenchmarkError {
    pub fn error(message: impl ToString) -> Self {
        Self {
            status: "error".into(),
            message: message.to_string(),
        }
    }
}

impl std::fmt::Display for BenchmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

impl std::error::Error for BenchmarkError {}

impl From<TryViaIrError> for BenchmarkError {
    fn from(error: TryViaIrError) -> Self {
        match error {
            TryViaIrError::Unsupported(message) => Self {
                status: "unsupported".into(),
                message,
            },
            TryViaIrError::Failed(message) => Self::error(message),
        }
    }
}

/// Reusable session and statistics cache for repetitions of one trusted read-only query.
pub struct BenchmarkHarness {
    session: SessionContext,
    statistics: RuntimeStatisticsCatalogBuilder,
}

impl BenchmarkHarness {
    pub fn new(session: SessionContext) -> Self {
        Self {
            statistics: RuntimeStatisticsCatalogBuilder::new(session.clone()),
            session,
        }
    }

    /// Plans from fresh optimizer state and executes a fresh direct physical plan once.
    ///
    /// Callers run warm-ups separately. Total planning includes cache lookups/statistics preparation,
    /// import, optimization, and physical planning; it excludes diagnostics and execution. A query
    /// error or unsupported shape is returned without fallback to a different engine path.
    pub async fn measure(&self, sql: &str) -> Result<BenchmarkSample, BenchmarkError> {
        let planning_started = Instant::now();
        let started = Instant::now();
        let plan = self
            .session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(BenchmarkError::error)?;
        let sql_planning_ms = elapsed_ms(started);

        let started = Instant::now();
        let optimizer =
            optimizer_context_for_benchmark(&self.session, &self.statistics, &plan).await?;
        let statistics_import_ms = elapsed_ms(started);

        let mut manager = default_pass_manager();
        let started = Instant::now();
        let planned = run_optimizer(optimizer, &mut manager).map_err(BenchmarkError::error)?;
        let optimizer_ms = elapsed_ms(started);

        let started = Instant::now();
        let physical =
            build_physical_ir(&planned, &self.session)
                .await
                .map_err(|error| match error {
                    ToPhysicalError::Unsupported(message) => BenchmarkError {
                        status: "unsupported".into(),
                        message,
                    },
                    error => BenchmarkError::error(error),
                })?;
        let physical_planning_ms = elapsed_ms(started);
        let planning_ms = elapsed_ms(planning_started);

        let task = self.session.task_ctx();
        let executable = physical.clone();
        let started = Instant::now();
        let batches = collect(executable, task)
            .await
            .map_err(BenchmarkError::error)?;
        let execution_ms = elapsed_ms(started);
        let output_rows = batches.iter().map(|batch| batch.num_rows()).sum();
        let physical_plan = displayable(physical.as_ref()).indent(true).to_string();
        let passes = manager
            .profiles()
            .iter()
            .map(|profile| BenchmarkPass {
                iteration: profile.iteration,
                pass_index: profile.pass_index,
                pass: profile.pass.to_string(),
                changed: profile
                    .result
                    .map(|result| result == optd_core::PassResult::Changed),
                duration_ms: profile.duration_ms,
            })
            .collect();
        Ok(BenchmarkSample {
            sql_planning_ms,
            statistics_import_ms,
            optimizer_ms,
            physical_planning_ms,
            planning_ms,
            execution_ms,
            output_rows,
            passes,
            physical_plan,
        })
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

/// Loads one SQL file or exactly one unconditional, successful SLT query record.
///
/// Rejects setup statements, include/connection directives, conditions, and multiple queries rather
/// than silently benchmarking only part of a test. Expected SLT output is not a correctness oracle
/// here: it is deliberately not compared in the timed collector.
pub fn load_benchmark_sql(path: &Path) -> Result<String, BenchmarkError> {
    let text = std::fs::read_to_string(path).map_err(BenchmarkError::error)?;
    let sql = match path.extension().and_then(|extension| extension.to_str()) {
        Some("sql") => text,
        Some("slt") => {
            if text.lines().filter(|line| line.trim() == "----").count() != 1 {
                return Err(BenchmarkError::error(
                    "expected exactly one SLT result separator",
                ));
            }
            let mut query = None;
            for record in
                sqllogictest::parse::<DFColumnType>(&text).map_err(BenchmarkError::error)?
            {
                match record {
                    Record::Query {
                        sql,
                        conditions,
                        connection: Connection::Default,
                        expected: QueryExpect::Results { .. },
                        retry: None,
                        ..
                    } if query.is_none() && conditions.is_empty() => query = Some(sql),
                    Record::Comment(_) | Record::Newline => {}
                    _ => {
                        return Err(BenchmarkError::error(
                            "expected exactly one unconditional SLT query, without setup or directives",
                        ));
                    }
                }
            }
            query.ok_or_else(|| BenchmarkError::error("SLT file has no query"))?
        }
        _ => return Err(BenchmarkError::error("expected a .sql or .slt file")),
    };
    if sql.trim().is_empty() {
        return Err(BenchmarkError::error("query is empty"));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OptdExtensionConfig;
    use datafusion::prelude::SessionConfig;

    #[tokio::test]
    async fn measures_production_direct_path_without_external_data() {
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_option_extension(OptdExtensionConfig {
                    optd_enabled: true,
                    physical_planning: true,
                    log_explain_steps: false,
                }),
        );
        session
            .sql("CREATE TABLE numbers AS SELECT * FROM (VALUES (1), (2), (3)) AS t(n)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let harness = BenchmarkHarness::new(session);
        for _ in 0..2 {
            let sample = harness
                .measure("SELECT n FROM numbers WHERE n > 1 ORDER BY n")
                .await
                .unwrap();
            assert_eq!(sample.output_rows, 2);
            assert!(!sample.passes.is_empty());
            assert!(!sample.physical_plan.is_empty());
            assert!(sample.planning_ms >= sample.optimizer_ms);
            for time in [
                sample.sql_planning_ms,
                sample.statistics_import_ms,
                sample.optimizer_ms,
                sample.physical_planning_ms,
                sample.planning_ms,
                sample.execution_ms,
            ] {
                assert!(time.is_finite() && time >= 0.0);
            }
        }
        assert!(
            harness
                .measure("SELECT * FROM missing_table")
                .await
                .is_err()
        );
        let window_sql = "SELECT n, row_number() OVER () AS rn FROM numbers";
        assert_eq!(
            harness.measure(window_sql).await.unwrap_err().status,
            "unsupported"
        );
        let window_plan = harness
            .session
            .state()
            .create_logical_plan(window_sql)
            .await
            .unwrap();
        // Benchmark coverage classification must not change production fallback semantics.
        assert!(matches!(
            crate::runner::optimizer_context_from_logical_plan(
                &harness.session,
                &harness.statistics,
                &window_plan
            )
            .await,
            Err(TryViaIrError::Failed(_))
        ));
        assert_eq!(
            harness
                .measure("EXPLAIN SELECT 1")
                .await
                .unwrap_err()
                .status,
            "unsupported"
        );
    }

    #[test]
    fn rejects_ambiguous_slt_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("query.slt");
        for text in [
            "query I\nSELECT 1\n",
            "query I\nSELECT 1\n----\n1\n\nquery I\nSELECT 2\n----\n2\n",
            "statement ok\nCREATE TABLE t(n INT);\n\nquery I\nSELECT 1\n----\n1\n",
            "skipif optd\nquery I\nSELECT 1\n----\n1\n",
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(load_benchmark_sql(&path).is_err());
        }
        std::fs::write(&path, "# comment\nquery I\nSELECT 1\n----\n1\n").unwrap();
        assert_eq!(load_benchmark_sql(&path).unwrap().trim(), "SELECT 1");
    }

    #[test]
    fn loads_all_existing_tpch_and_job_result_queries() {
        for suite in ["tpch", "job"] {
            let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/slt")
                .join(suite)
                .join("results");
            let mut count = 0;
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|extension| extension == "slt") {
                    load_benchmark_sql(&path)
                        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
                    count += 1;
                }
            }
            assert!(count > 0);
        }
    }
}
