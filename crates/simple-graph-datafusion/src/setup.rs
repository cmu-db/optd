//! Session setup: generates TPC-H data at SF=0.1 into temp `.tbl` files
//! and registers them as CSV tables with `|` delimiter.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result as DFResult;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use tempfile::TempDir;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

const SF: f64 = 0.1;

/// Creates a `SessionContext` with all TPC-H tables registered at SF=0.1.
///
/// Data is generated into a temporary directory and registered as `|`-delimited
/// CSV tables. The `TempDir` is returned alongside the context so the caller
/// can keep it alive for the duration of the session.
pub async fn setup_tpch_session() -> DFResult<(SessionContext, TempDir)> {
    let dir = tempfile::tempdir()?;
    write_tbl(&dir, "lineitem", LineItemGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "orders", OrderGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "customer", CustomerGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "part", PartGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "partsupp", PartSuppGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "supplier", SupplierGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "nation", NationGenerator::new(SF, 1, 1).iter())?;
    write_tbl(&dir, "region", RegionGenerator::new(SF, 1, 1).iter())?;

    let ctx = SessionContext::new();
    let opts = CsvReadOptions::new().delimiter(b'|').has_header(false);

    for (name, schema) in tpch_schemas() {
        let path = dir.path().join(format!("{name}.csv"));
        ctx.register_csv(name, path.to_str().unwrap(), opts.clone().schema(&schema))
            .await?;
    }

    Ok((ctx, dir))
}

fn write_tbl<T: std::fmt::Display>(
    dir: &TempDir,
    name: &str,
    rows: impl Iterator<Item = T>,
) -> DFResult<()> {
    let path = dir.path().join(format!("{name}.csv"));
    let mut w = BufWriter::new(File::create(path)?);
    for row in rows {
        // TBL format has a trailing '|'; strip it so CSV parser sees N columns.
        let s = row.to_string();
        writeln!(w, "{}", s.trim_end_matches('|'))?;
    }
    Ok(())
}

fn tpch_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (
            "lineitem",
            Schema::new(vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_partkey", DataType::Int64, false),
                Field::new("l_suppkey", DataType::Int64, false),
                Field::new("l_linenumber", DataType::Int32, false),
                Field::new("l_quantity", DataType::Decimal128(15, 2), false),
                Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
                Field::new("l_discount", DataType::Decimal128(15, 2), false),
                Field::new("l_tax", DataType::Decimal128(15, 2), false),
                Field::new("l_returnflag", DataType::Utf8, false),
                Field::new("l_linestatus", DataType::Utf8, false),
                Field::new("l_shipdate", DataType::Date32, false),
                Field::new("l_commitdate", DataType::Date32, false),
                Field::new("l_receiptdate", DataType::Date32, false),
                Field::new("l_shipinstruct", DataType::Utf8, false),
                Field::new("l_shipmode", DataType::Utf8, false),
                Field::new("l_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "orders",
            Schema::new(vec![
                Field::new("o_orderkey", DataType::Int64, false),
                Field::new("o_custkey", DataType::Int64, false),
                Field::new("o_orderstatus", DataType::Utf8, false),
                Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
                Field::new("o_orderdate", DataType::Date32, false),
                Field::new("o_orderpriority", DataType::Utf8, false),
                Field::new("o_clerk", DataType::Utf8, false),
                Field::new("o_shippriority", DataType::Int32, false),
                Field::new("o_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "customer",
            Schema::new(vec![
                Field::new("c_custkey", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
                Field::new("c_address", DataType::Utf8, false),
                Field::new("c_nationkey", DataType::Int64, false),
                Field::new("c_phone", DataType::Utf8, false),
                Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
                Field::new("c_mktsegment", DataType::Utf8, false),
                Field::new("c_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "part",
            Schema::new(vec![
                Field::new("p_partkey", DataType::Int64, false),
                Field::new("p_name", DataType::Utf8, false),
                Field::new("p_mfgr", DataType::Utf8, false),
                Field::new("p_brand", DataType::Utf8, false),
                Field::new("p_type", DataType::Utf8, false),
                Field::new("p_size", DataType::Int32, false),
                Field::new("p_container", DataType::Utf8, false),
                Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
                Field::new("p_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "partsupp",
            Schema::new(vec![
                Field::new("ps_partkey", DataType::Int64, false),
                Field::new("ps_suppkey", DataType::Int64, false),
                Field::new("ps_availqty", DataType::Int32, false),
                Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
                Field::new("ps_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "supplier",
            Schema::new(vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_name", DataType::Utf8, false),
                Field::new("s_address", DataType::Utf8, false),
                Field::new("s_nationkey", DataType::Int64, false),
                Field::new("s_phone", DataType::Utf8, false),
                Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
                Field::new("s_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "nation",
            Schema::new(vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, false),
                Field::new("n_regionkey", DataType::Int64, false),
                Field::new("n_comment", DataType::Utf8, false),
            ]),
        ),
        (
            "region",
            Schema::new(vec![
                Field::new("r_regionkey", DataType::Int64, false),
                Field::new("r_name", DataType::Utf8, false),
                Field::new("r_comment", DataType::Utf8, false),
            ]),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lineitem_count_at_sf01() {
        let (ctx, _dir) = setup_tpch_session().await.unwrap();
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
}

#[tokio::test]
async fn to_logical_plan_round_trip_nation() {
    use crate::from_df::from_logical_plan;
    use crate::to_df::to_logical_plan;
    use simple_graph::{OptimizerContext, QueryContext};

    let (session, _dir) = setup_tpch_session().await.unwrap();
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

    let (session, _dir) = setup_tpch_session().await.unwrap();
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
