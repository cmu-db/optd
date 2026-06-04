//! Session setup for local benchmark Parquet data.
//!
//! Generate the data with:
//!
//! ```text
//! ./scripts/generate_tpch.sh
//! ```

use std::path::PathBuf;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

use crate::config::OptdExtensionConfig;

const TPCH_DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch/sf-0.1");
const JOB_DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/job");
const TPCH_TABLES: &[&str] = &[
    "lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region",
];
const JOB_TABLES: &[&str] = &[
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title",
];

/// Creates a `SessionContext` with all TPC-H tables registered from local Parquet files.
pub async fn setup_tpch_session() -> DFResult<SessionContext> {
    let ctx = session_context_with_information_schema();
    register_tpch_tables(&ctx).await?;
    Ok(ctx)
}

/// Registers all TPC-H tables from local Parquet files.
pub async fn register_tpch_tables(ctx: &SessionContext) -> DFResult<()> {
    register_parquet_tables(
        ctx,
        TPCH_TABLES,
        TPCH_DATA_DIR,
        "TPC-H",
        "./scripts/generate_tpch.sh",
    )
    .await
}

/// Creates a `SessionContext` with all JOB tables registered from local Parquet files.
pub async fn setup_job_session() -> DFResult<SessionContext> {
    let ctx = session_context_with_information_schema();
    register_job_tables(&ctx).await?;
    Ok(ctx)
}

/// Registers all JOB tables from local Parquet files.
pub async fn register_job_tables(ctx: &SessionContext) -> DFResult<()> {
    register_parquet_tables(
        ctx,
        JOB_TABLES,
        JOB_DATA_DIR,
        "JOB",
        "./scripts/generate_job_parquet.sh",
    )
    .await
}

async fn register_parquet_tables(
    ctx: &SessionContext,
    tables: &[&str],
    data_dir: &str,
    dataset_name: &str,
    generate_command: &str,
) -> DFResult<()> {
    for table in tables {
        let path = parquet_path(data_dir, table);
        if !path.exists() {
            return Err(DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "missing {dataset_name} parquet data at {}. Generate it with: {generate_command}",
                    path.display(),
                ),
            ))));
        }

        ctx.register_parquet(
            *table,
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
    }

    Ok(())
}

/// Creates a `SessionContext` configured for interactive CLI use.
pub fn session_context_with_information_schema() -> SessionContext {
    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_option_extension(OptdExtensionConfig::default());
    SessionContext::new_with_config(config)
}

fn parquet_path(data_dir: &str, table: &str) -> PathBuf {
    PathBuf::from(data_dir).join(format!("{table}.parquet"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OptdExtensionConfig;

    #[test]
    fn session_context_registers_optd_extension_defaults() {
        let ctx = session_context_with_information_schema();
        let config = ctx.copied_config();
        let optd = config
            .options()
            .extensions
            .get::<OptdExtensionConfig>()
            .unwrap();
        assert!(optd.optd_enabled);
        assert!(optd.log_explain_steps);
    }

    #[tokio::test]
    async fn lineitem_count_at_sf01() {
        let ctx = setup_tpch_session().await.unwrap();
        let df = ctx.sql("SELECT COUNT(*) FROM lineitem").await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 600572);
    }

    #[tokio::test]
    async fn to_logical_plan_round_trip_nation() {
        use crate::from_df::from_logical_plan;
        use crate::to_df::to_logical_plan;
        use optd_core::{OptimizerContext, QueryContext};

        let session = setup_tpch_session().await.unwrap();
        let plan = session
            .state()
            .create_logical_plan("SELECT COUNT(*) FROM nation")
            .await
            .unwrap();

        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        ctx.set_root(root);
        let opt_ctx = OptimizerContext::new(ctx);
        let ctx = opt_ctx.into_query();
        let df_plan = to_logical_plan(&ctx, &session).await.unwrap();
        let df = session.execute_logical_plan(df_plan).await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 25);
    }

    #[tokio::test]
    async fn print_q13_ir() {
        use crate::from_df::from_logical_plan;
        use optd_core::QueryContext;

        let session = setup_tpch_session().await.unwrap();
        let sql = "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc";

        let plan = session.state().create_logical_plan(sql).await.unwrap();
        println!("=== DataFusion plan ===\n{}", plan.display_indent());

        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        ctx.set_root(root);
        println!(
            "=== optd IR ===\n{}",
            ctx.pretty_with_config(
                optd_core::QueryFormatConfig::new().with_analysis::<optd_core::AvailableColumns>()
            )
        );
    }
}
