use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TpchQueryStatus {
    exports: bool,
    datafusion_consumes: bool,
    schema_compatible: bool,
}

impl TpchQueryStatus {
    const PENDING: Self = Self {
        exports: false,
        datafusion_consumes: false,
        schema_compatible: false,
    };
}

#[derive(Debug, Clone, Copy)]
struct TpchQueryMatrixRow {
    query: u8,
    status: TpchQueryStatus,
}

#[test]
fn tpch_query_matrix_covers_q1_through_q22() {
    let matrix = tpch_query_matrix();

    assert_eq!(matrix.len(), 22);
    for query in 1..=22 {
        let row = matrix
            .iter()
            .find(|row| row.query == query)
            .unwrap_or_else(|| panic!("missing TPC-H Q{query} matrix row"));

        assert!(
            !row.status.schema_compatible || row.status.datafusion_consumes,
            "Q{query} cannot be schema-compatible before DataFusion consumes it"
        );
        assert!(
            !row.status.datafusion_consumes || row.status.exports,
            "Q{query} cannot be consumed before export succeeds"
        );
    }
}

#[test]
fn tpch_empty_datafusion_context_registers_all_benchmark_tables() {
    let ctx = tpch_session_context();

    for table in [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ] {
        assert!(
            ctx.table_exist(table).unwrap(),
            "TPC-H table should be registered: {table}"
        );
    }
}

fn tpch_query_matrix() -> Vec<TpchQueryMatrixRow> {
    (1..=22)
        .map(|query| TpchQueryMatrixRow {
            query,
            status: TpchQueryStatus::PENDING,
        })
        .collect()
}

fn tpch_session_context() -> SessionContext {
    let ctx = SessionContext::new();
    for (name, schema) in tpch_schemas() {
        let schema = Arc::new(schema);
        let batch = empty_batch(Arc::clone(&schema));
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(name, Arc::new(table)).unwrap();
    }
    ctx
}

fn empty_batch(schema: Arc<Schema>) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).unwrap()
}

fn tpch_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (
            "customer",
            Schema::new(vec![
                int64("c_custkey"),
                utf8("c_name"),
                utf8("c_address"),
                int64("c_nationkey"),
                utf8("c_phone"),
                decimal("c_acctbal"),
                utf8("c_mktsegment"),
                utf8("c_comment"),
            ]),
        ),
        (
            "lineitem",
            Schema::new(vec![
                int64("l_orderkey"),
                int64("l_partkey"),
                int64("l_suppkey"),
                int32("l_linenumber"),
                decimal("l_quantity"),
                decimal("l_extendedprice"),
                decimal("l_discount"),
                decimal("l_tax"),
                utf8("l_returnflag"),
                utf8("l_linestatus"),
                date("l_shipdate"),
                date("l_commitdate"),
                date("l_receiptdate"),
                utf8("l_shipinstruct"),
                utf8("l_shipmode"),
                utf8("l_comment"),
            ]),
        ),
        (
            "nation",
            Schema::new(vec![
                int64("n_nationkey"),
                utf8("n_name"),
                int64("n_regionkey"),
                utf8("n_comment"),
            ]),
        ),
        (
            "orders",
            Schema::new(vec![
                int64("o_orderkey"),
                int64("o_custkey"),
                utf8("o_orderstatus"),
                decimal("o_totalprice"),
                date("o_orderdate"),
                utf8("o_orderpriority"),
                utf8("o_clerk"),
                int32("o_shippriority"),
                utf8("o_comment"),
            ]),
        ),
        (
            "part",
            Schema::new(vec![
                int64("p_partkey"),
                utf8("p_name"),
                utf8("p_mfgr"),
                utf8("p_brand"),
                utf8("p_type"),
                int32("p_size"),
                utf8("p_container"),
                decimal("p_retailprice"),
                utf8("p_comment"),
            ]),
        ),
        (
            "partsupp",
            Schema::new(vec![
                int64("ps_partkey"),
                int64("ps_suppkey"),
                int32("ps_availqty"),
                decimal("ps_supplycost"),
                utf8("ps_comment"),
            ]),
        ),
        (
            "region",
            Schema::new(vec![
                int64("r_regionkey"),
                utf8("r_name"),
                utf8("r_comment"),
            ]),
        ),
        (
            "supplier",
            Schema::new(vec![
                int64("s_suppkey"),
                utf8("s_name"),
                utf8("s_address"),
                int64("s_nationkey"),
                utf8("s_phone"),
                decimal("s_acctbal"),
                utf8("s_comment"),
            ]),
        ),
    ]
}

fn int32(name: &'static str) -> Field {
    Field::new(name, DataType::Int32, false)
}

fn int64(name: &'static str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn utf8(name: &'static str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn decimal(name: &'static str) -> Field {
    Field::new(name, DataType::Decimal128(15, 2), false)
}

fn date(name: &'static str) -> Field {
    Field::new(name, DataType::Date32, false)
}
