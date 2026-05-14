use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};

const PLAN_ONLY_MARKER: &str = "-- simple_graph: plan-only";

#[derive(Debug)]
struct SubstraitRoundTripQueryPlanner;

#[async_trait]
impl QueryPlanner for SubstraitRoundTripQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let substrait_plan = datafusion_substrait::logical_plan::producer::to_substrait_plan(
            logical_plan,
            session_state,
        )?;
        let round_tripped_plan = datafusion_substrait::logical_plan::consumer::from_substrait_plan(
            session_state,
            &substrait_plan,
        )
        .await?;

        DefaultPhysicalPlanner::default()
            .create_physical_plan(&round_tripped_plan, session_state)
            .await
    }
}

struct DataFusionSubstraitDb {
    ctx: SessionContext,
}

impl DataFusionSubstraitDb {
    fn new() -> DataFusionResult<Self> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rules(vec![])
            .with_query_planner(Arc::new(SubstraitRoundTripQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);

        register_numbers_table(&ctx)?;
        register_tpch_tables(&ctx)?;
        Ok(Self { ctx })
    }
}

#[async_trait]
impl AsyncDB for DataFusionSubstraitDb {
    type Error = datafusion::error::DataFusionError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        if let Some(plan_only_sql) = strip_plan_only_marker(sql) {
            let dataframe = self.ctx.sql(plan_only_sql).await?;
            let (state, logical_plan) = dataframe.into_parts();
            state.create_physical_plan(&logical_plan).await?;
            return Ok(DBOutput::StatementComplete(0));
        }

        let dataframe = self.ctx.sql(sql).await?;
        let schema = dataframe.schema().clone();
        let batches = dataframe.collect().await?;
        let row_count = batches.iter().map(|batch| batch.num_rows() as u64).sum();

        if schema.fields().is_empty() {
            return Ok(DBOutput::StatementComplete(row_count));
        }

        let types = schema
            .fields()
            .iter()
            .map(|field| column_type(field.data_type()))
            .collect();
        let mut rows = Vec::new();

        for batch in batches {
            for row_index in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for column in batch.columns() {
                    let value = ScalarValue::try_from_array(column.as_ref(), row_index)?;
                    row.push(value.to_string());
                }
                rows.push(row);
            }
        }

        Ok(DBOutput::Rows { types, rows })
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn sqllogictest_runs_datafusion_substrait_roundtrip() {
    let mut runner = Runner::new(|| async { DataFusionSubstraitDb::new() });

    runner
        .run_file_async("tests/slt/datafusion_substrait_roundtrip.slt")
        .await
        .unwrap();
}

#[test]
fn tpch_sqllogictest_contains_all_queries() {
    let script = include_str!("slt/tpch_roundtrip_plan_only.slt");

    for query in 1..=22 {
        assert!(
            script.contains(&format!("# TPC-H Q{query}")),
            "missing TPC-H Q{query} sqllogictest entry"
        );
    }
}

#[tokio::test]
async fn sqllogictest_runs_tpch_substrait_roundtrip_plan_only() {
    if std::env::var("INCLUDE_TPCH").as_deref() != Ok("true") {
        return;
    }

    let mut runner = Runner::new(|| async { DataFusionSubstraitDb::new() });

    runner
        .run_file_async("tests/slt/tpch_roundtrip_plan_only.slt")
        .await
        .unwrap();
}

fn register_numbers_table(ctx: &SessionContext) -> DataFusionResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["one", "two", "three"])),
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("numbers", Arc::new(table))?;
    Ok(())
}

fn register_tpch_tables(ctx: &SessionContext) -> DataFusionResult<()> {
    register_empty_table(
        ctx,
        "customer",
        vec![
            i64_field("c_custkey"),
            string_field("c_name"),
            string_field("c_address"),
            i64_field("c_nationkey"),
            string_field("c_phone"),
            decimal_field("c_acctbal"),
            string_field("c_mktsegment"),
            string_field("c_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "lineitem",
        vec![
            i64_field("l_orderkey"),
            i64_field("l_partkey"),
            i64_field("l_suppkey"),
            i64_field("l_linenumber"),
            decimal_field("l_quantity"),
            decimal_field("l_extendedprice"),
            decimal_field("l_discount"),
            decimal_field("l_tax"),
            string_field("l_returnflag"),
            string_field("l_linestatus"),
            date_field("l_shipdate"),
            date_field("l_commitdate"),
            date_field("l_receiptdate"),
            string_field("l_shipinstruct"),
            string_field("l_shipmode"),
            string_field("l_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "nation",
        vec![
            i64_field("n_nationkey"),
            string_field("n_name"),
            i64_field("n_regionkey"),
            string_field("n_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "orders",
        vec![
            i64_field("o_orderkey"),
            i64_field("o_custkey"),
            string_field("o_orderstatus"),
            decimal_field("o_totalprice"),
            date_field("o_orderdate"),
            string_field("o_orderpriority"),
            string_field("o_clerk"),
            i64_field("o_shippriority"),
            string_field("o_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "part",
        vec![
            i64_field("p_partkey"),
            string_field("p_name"),
            string_field("p_mfgr"),
            string_field("p_brand"),
            string_field("p_type"),
            i64_field("p_size"),
            string_field("p_container"),
            decimal_field("p_retailprice"),
            string_field("p_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "partsupp",
        vec![
            i64_field("ps_partkey"),
            i64_field("ps_suppkey"),
            i64_field("ps_availqty"),
            decimal_field("ps_supplycost"),
            string_field("ps_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "region",
        vec![
            i64_field("r_regionkey"),
            string_field("r_name"),
            string_field("r_comment"),
        ],
    )?;
    register_empty_table(
        ctx,
        "supplier",
        vec![
            i64_field("s_suppkey"),
            string_field("s_name"),
            string_field("s_address"),
            i64_field("s_nationkey"),
            string_field("s_phone"),
            decimal_field("s_acctbal"),
            string_field("s_comment"),
        ],
    )?;

    Ok(())
}

fn register_empty_table(
    ctx: &SessionContext,
    name: &str,
    fields: Vec<Field>,
) -> DataFusionResult<()> {
    let schema = Arc::new(Schema::new(fields));
    let table = MemTable::try_new(
        Arc::clone(&schema),
        vec![vec![RecordBatch::new_empty(schema)]],
    )?;

    ctx.register_table(name, Arc::new(table))?;
    Ok(())
}

fn i64_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(15, 2), false)
}

fn date_field(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

fn string_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn strip_plan_only_marker(sql: &str) -> Option<&str> {
    let sql = sql.trim_start();
    sql.strip_prefix(PLAN_ONLY_MARKER).map(str::trim_start)
}

fn column_type(data_type: &DataType) -> DefaultColumnType {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DefaultColumnType::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => DefaultColumnType::FloatingPoint,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}
