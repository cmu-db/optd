//! One-query worker; use scripts/run_query_benchmarks.py for isolated suite collection.

use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_datafusion::benchmark::{
    BenchmarkError, BenchmarkHarness, BenchmarkSample, load_benchmark_sql,
};
use optd_datafusion::config::OptdExtensionConfig;
use optd_datafusion::setup::{
    JOB_DATA_DIR, TPCH_DATA_DIR, register_job_tables, register_tpch_tables,
};
use serde::Serialize;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Dataset {
    Tpch,
    Job,
}

#[derive(Debug, Parser)]
#[command(about = "Measure one trusted TPC-H/JOB query on the direct physical optd path")]
struct Args {
    #[arg(long, value_enum)]
    dataset: Dataset,
    /// Exactly one .sql file or a single-query .slt fixture.
    #[arg(long)]
    query: PathBuf,
    /// Raw checkpoint report, not the action's scalar input.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    samples: u32,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    warmups: u32,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    target_partitions: u32,
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    status: String,
    error: Option<String>,
    execution_path: &'static str,
    dataset: &'static str,
    dataset_directory: &'static str,
    correctness: &'static str,
    statistics: &'static str,
    warmups_completed: u32,
    samples: Vec<BenchmarkSample>,
}

fn checkpoint(path: &Path, report: &Report) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(report)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

async fn measure(args: &Args, report: &mut Report) -> Result<(), BenchmarkError> {
    let sql = load_benchmark_sql(&args.query)?;
    let mut optd_config = OptdExtensionConfig::default();
    optd_config.optd_enabled = true;
    optd_config.physical_planning = true;
    optd_config.log_explain_steps = false;
    let session = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(args.target_partitions as usize)
            .with_option_extension(optd_config),
    );
    match args.dataset {
        Dataset::Tpch => register_tpch_tables(&session).await,
        Dataset::Job => register_job_tables(&session).await,
    }
    .map_err(BenchmarkError::error)?;
    let harness = BenchmarkHarness::new(session);
    let mut rows = None;
    for _ in 0..args.warmups {
        let sample = harness.measure(&sql).await?;
        if rows.is_some_and(|rows| rows != sample.output_rows) {
            return Err(BenchmarkError::error(
                "output row count changed between repetitions",
            ));
        }
        rows = Some(sample.output_rows);
        report.warmups_completed += 1;
        checkpoint(&args.output, report).map_err(BenchmarkError::error)?;
    }
    for _ in 0..args.samples {
        let sample = harness.measure(&sql).await?;
        if rows != Some(sample.output_rows) {
            return Err(BenchmarkError::error(
                "output row count changed between repetitions",
            ));
        }
        report.samples.push(sample);
        checkpoint(&args.output, report).map_err(BenchmarkError::error)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut report = Report {
        schema_version: 1,
        status: "running".into(),
        error: None,
        execution_path: "direct_physical",
        dataset: match args.dataset {
            Dataset::Tpch => "tpch",
            Dataset::Job => "job",
        },
        dataset_directory: match args.dataset {
            Dataset::Tpch => TPCH_DATA_DIR,
            Dataset::Job => JOB_DATA_DIR,
        },
        correctness: "not_checked",
        statistics: "runtime_aggregate_cache_warmed_by_query_warmups",
        warmups_completed: 0,
        samples: Vec::new(),
    };
    checkpoint(&args.output, &report)?;
    let result = measure(&args, &mut report).await;
    match &result {
        Ok(()) => report.status = "success".into(),
        Err(error) => {
            report.status.clone_from(&error.status);
            report.error = Some(error.message.clone());
        }
    }
    checkpoint(&args.output, &report)?;
    result.map_err(Into::into)
}
