use std::error::Error;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_datafusion::cardinality_regression::{
    CardinalityRegressionHarness, load_query_specs, write_report,
};
use optd_datafusion::config::OptdExtensionConfig;
use optd_datafusion::setup::{register_job_tables, register_tpch_tables};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Dataset {
    Tpch,
    Job,
}

#[derive(Debug, Parser)]
#[command(
    name = "cardinality-regression",
    about = "Measure per-subtree cardinality q-error"
)]
struct Args {
    /// Benchmark dataset whose local Parquet tables should be registered.
    #[arg(long, value_enum)]
    dataset: Dataset,

    /// One .sql/.slt file or a directory containing query files.
    #[arg(long)]
    queries: PathBuf,

    /// Directory for report.json.
    #[arg(long, default_value = "target/cardinality-regression")]
    output: PathBuf,

    /// Measure only the first N naturally sorted query files.
    #[arg(long)]
    limit: Option<usize>,

    /// DataFusion target partition count used while executing exact subtree probes.
    #[arg(long)]
    target_partitions: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut config = SessionConfig::new()
        .with_information_schema(true)
        .with_option_extension(OptdExtensionConfig::default());
    if let Some(target_partitions) = args.target_partitions {
        config = config.with_target_partitions(target_partitions);
    }
    let session = SessionContext::new_with_config(config);
    match args.dataset {
        Dataset::Tpch => register_tpch_tables(&session).await?,
        Dataset::Job => register_job_tables(&session).await?,
    }

    let mut queries = load_query_specs(&args.queries)?;
    if let Some(limit) = args.limit {
        queries.truncate(limit);
    }
    if queries.is_empty() {
        return Err("query limit selected no queries".into());
    }

    let harness = CardinalityRegressionHarness::new(session);
    let mut measurements = Vec::new();
    for query in &queries {
        eprintln!("measuring {}", query.name);
        measurements.extend(harness.measure_query(&query.name, &query.sql).await?);
    }
    let report = write_report(&measurements, &args.output)?;
    eprintln!("wrote {}", report.display());
    Ok(())
}
