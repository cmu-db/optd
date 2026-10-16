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
    All,
}

impl Dataset {
    fn name(self) -> &'static str {
        match self {
            Self::Tpch => "tpch",
            Self::Job => "job",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "cardinality-regression",
    about = "Measure per-subtree cardinality q-error"
)]
struct Args {
    /// Benchmark dataset whose local Parquet tables should be registered.
    #[arg(long, value_enum, default_value = "all")]
    dataset: Dataset,

    /// Override the query path when exactly one dataset is selected.
    #[arg(long)]
    queries: Option<PathBuf>,

    /// TPC-H .sql/.slt file or query directory.
    #[arg(
        long,
        default_value = "optd/connectors/datafusion/tests/slt/tpch/results"
    )]
    tpch_queries: PathBuf,

    /// JOB .sql/.slt file or query directory.
    #[arg(
        long,
        default_value = "optd/connectors/datafusion/tests/slt/job/results"
    )]
    job_queries: PathBuf,

    /// Directory for report.json.
    #[arg(long, default_value = "target/cardinality-regression")]
    output: PathBuf,

    /// Measure only the first N naturally sorted query files from each selected suite.
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
        Dataset::All => {
            register_tpch_tables(&session).await?;
            register_job_tables(&session).await?;
        }
    }
    if args.queries.is_some() && matches!(args.dataset, Dataset::All) {
        return Err("--queries requires --dataset tpch or --dataset job".into());
    }

    let selected_suites = match args.dataset {
        Dataset::Tpch => vec![(
            Dataset::Tpch,
            args.queries.as_ref().unwrap_or(&args.tpch_queries),
        )],
        Dataset::Job => vec![(
            Dataset::Job,
            args.queries.as_ref().unwrap_or(&args.job_queries),
        )],
        Dataset::All => vec![
            (Dataset::Tpch, &args.tpch_queries),
            (Dataset::Job, &args.job_queries),
        ],
    };

    let harness = CardinalityRegressionHarness::new(session);
    let mut measurements = Vec::new();
    for (dataset, query_path) in selected_suites {
        let mut queries = load_query_specs(query_path)?;
        if let Some(limit) = args.limit {
            queries.truncate(limit);
        }
        if queries.is_empty() {
            return Err(format!("{} query selection contains no queries", dataset.name()).into());
        }
        for query in &queries {
            eprintln!("measuring {}/{}", dataset.name(), query.name);
            let mut query_measurements = harness.measure_query(&query.name, &query.sql).await?;
            for measurement in &mut query_measurements {
                measurement.suite = dataset.name().to_string();
            }
            measurements.extend(query_measurements);
        }
    }
    let report = write_report(&measurements, &args.output)?;
    eprintln!("wrote {}", report.display());
    Ok(())
}
