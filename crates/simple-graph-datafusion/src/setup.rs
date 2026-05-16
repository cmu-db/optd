//! Session setup for local TPC-H Parquet data.
//!
//! Generate the data with:
//!
//! ```text
//! tpchgen-cli -s 0.1 -f parquet -o crates/simple-graph-datafusion/data/tpch/sf-0.1
//! ```

use std::path::PathBuf;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{ParquetReadOptions, SessionContext};

const TPCH_DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch/sf-0.1");
const TPCH_TABLES: &[&str] = &[
    "lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region",
];

/// Creates a `SessionContext` with all TPC-H tables registered from local Parquet files.
pub async fn setup_tpch_session() -> DFResult<SessionContext> {
    let ctx = SessionContext::new();

    for table in TPCH_TABLES {
        let path = tpch_parquet_path(table);
        if !path.exists() {
            return Err(DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "missing TPC-H parquet data at {}. Generate it with: \
                     tpchgen-cli -s 0.1 -f parquet -o crates/simple-graph-datafusion/data/tpch/sf-0.1",
                    path.display()
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

    Ok(ctx)
}

fn tpch_parquet_path(table: &str) -> PathBuf {
    PathBuf::from(TPCH_DATA_DIR).join(format!("{table}.parquet"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        use simple_graph::{OptimizerContext, QueryContext};

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
        use simple_graph::QueryContext;

        let session = setup_tpch_session().await.unwrap();
        let sql = "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc";

        let plan = session.state().create_logical_plan(sql).await.unwrap();
        println!("=== DataFusion plan ===\n{}", plan.display_indent());

        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        ctx.set_root(root);
        println!(
            "=== simple-graph IR ===\n{}",
            ctx.pretty_with_config(
                simple_graph::QueryFormatConfig::new()
                    .with_analysis::<simple_graph::AvailableColumns>()
            )
        );
    }
}
