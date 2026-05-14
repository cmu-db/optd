use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::datatypes::{DataType as DataFusionDataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use prost::Message;
use simple_graph::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, ColumnData, ExprData, Map, NaryOp,
    OperatorData, Output, Projection, QueryContext, ScalarValue, Scan, Selection, TableRef,
    substrait,
};

type QueryBuilder = fn() -> QueryContext;

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
    build: Option<QueryBuilder>,
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

#[tokio::test]
async fn tpch_implemented_matrix_rows_export_and_datafusion_consumes() {
    let df_ctx = tpch_session_context();

    for row in tpch_query_matrix()
        .into_iter()
        .filter(|row| row.status.exports)
    {
        let query = row
            .build
            .unwrap_or_else(|| panic!("Q{} is marked exportable without a builder", row.query))(
        );
        let plan = substrait::to_plan(&query).unwrap();

        let mut bytes = Vec::new();
        plan.encode(&mut bytes).unwrap();

        let df_substrait_plan = datafusion_substrait::serializer::deserialize_bytes(bytes)
            .await
            .unwrap();
        let logical_plan = datafusion_substrait::logical_plan::consumer::from_substrait_plan(
            &df_ctx.state(),
            &df_substrait_plan,
        )
        .await
        .unwrap();

        if row.status.schema_compatible {
            assert_expected_schema(row.query, logical_plan.schema().fields());
        }
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
    let builders: [QueryBuilder; 22] = [
        tpch_q1, tpch_q2, tpch_q3, tpch_q4, tpch_q5, tpch_q6, tpch_q7, tpch_q8, tpch_q9, tpch_q10,
        tpch_q11, tpch_q12, tpch_q13, tpch_q14, tpch_q15, tpch_q16, tpch_q17, tpch_q18, tpch_q19,
        tpch_q20, tpch_q21, tpch_q22,
    ];

    builders
        .into_iter()
        .enumerate()
        .map(|(index, build)| {
            let query = index as u8 + 1;
            TpchQueryMatrixRow {
                query,
                status: if query == 6 {
                    TpchQueryStatus {
                        exports: true,
                        datafusion_consumes: true,
                        schema_compatible: true,
                    }
                } else {
                    TpchQueryStatus::PENDING
                },
                build: Some(build),
            }
        })
        .collect()
}

#[test]
fn tpch_query_matrix_has_direct_ir_builder_for_every_query() {
    for row in tpch_query_matrix() {
        let query = row
            .build
            .unwrap_or_else(|| panic!("Q{} is missing a direct IR builder", row.query))(
        );
        let root = query
            .root()
            .unwrap_or_else(|| panic!("Q{} builder did not set a root", row.query));

        assert!(
            matches!(query.operator(root), OperatorData::Output(_)),
            "Q{} builder root should be Output",
            row.query
        );

        // Exercise display traversal while builders are still being filled in.
        let rendered = query.pretty_flat();
        assert!(
            rendered.contains("\"kind\": \"output\""),
            "Q{} builder should render an output node: {rendered}",
            row.query
        );
    }
}

fn assert_expected_schema(query: u8, fields: &Fields) {
    match query {
        6 => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name(), "revenue");
            assert_eq!(
                fields[0].data_type(),
                &DataFusionDataType::Decimal128(38, 4)
            );
        }
        _ => panic!("no expected schema registered for Q{query}"),
    }
}

fn tpch_q1() -> QueryContext {
    tpch_schema_only_query(
        "lineitem",
        vec![
            utf8_ty("l_returnflag"),
            utf8_ty("l_linestatus"),
            decimal_ty("sum_qty"),
            decimal_ty("sum_base_price"),
            decimal_ty("sum_disc_price"),
            decimal_ty("sum_charge"),
            decimal_ty("avg_qty"),
            decimal_ty("avg_price"),
            decimal_ty("avg_disc"),
            int64_ty("count_order"),
        ],
    )
}

fn tpch_q2() -> QueryContext {
    tpch_schema_only_query(
        "part",
        vec![
            decimal_ty("s_acctbal"),
            utf8_ty("s_name"),
            utf8_ty("n_name"),
            int64_ty("p_partkey"),
            utf8_ty("p_mfgr"),
            utf8_ty("s_address"),
            utf8_ty("s_phone"),
            utf8_ty("s_comment"),
        ],
    )
}

fn tpch_q3() -> QueryContext {
    tpch_schema_only_query(
        "lineitem",
        vec![
            int64_ty("l_orderkey"),
            decimal_ty("revenue"),
            date_ty("o_orderdate"),
            int32_ty("o_shippriority"),
        ],
    )
}

fn tpch_q4() -> QueryContext {
    tpch_schema_only_query(
        "orders",
        vec![utf8_ty("o_orderpriority"), int64_ty("order_count")],
    )
}

fn tpch_q5() -> QueryContext {
    tpch_schema_only_query("nation", vec![utf8_ty("n_name"), decimal_ty("revenue")])
}

fn tpch_q6() -> QueryContext {
    let mut ctx = QueryContext::new();
    let shipdate = ctx.add_column(ColumnData::new(
        "l_shipdate",
        arrow_schema::DataType::Date32,
    ));
    let discount = ctx.add_column(ColumnData::new(
        "l_discount",
        arrow_schema::DataType::Decimal128(15, 2),
    ));
    let quantity = ctx.add_column(ColumnData::new(
        "l_quantity",
        arrow_schema::DataType::Decimal128(15, 2),
    ));
    let extendedprice = ctx.add_column(ColumnData::new(
        "l_extendedprice",
        arrow_schema::DataType::Decimal128(15, 2),
    ));
    let revenue = ctx.add_column(ColumnData::new(
        "revenue",
        arrow_schema::DataType::Decimal128(15, 2),
    ));

    let lineitem = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("lineitem"),
        columns: vec![shipdate, discount, quantity, extendedprice],
    }));
    let shipdate_ref = ctx.add_expr(ExprData::ColumnRef(shipdate));
    let start_date = ctx.add_expr(ExprData::Literal(ScalarValue::Date32(8766)));
    let shipdate_after_start = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::GtEq,
        left: shipdate_ref,
        right: start_date,
    });
    let shipdate_ref = ctx.add_expr(ExprData::ColumnRef(shipdate));
    let end_date = ctx.add_expr(ExprData::Literal(ScalarValue::Date32(9131)));
    let shipdate_before_end = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Lt,
        left: shipdate_ref,
        right: end_date,
    });
    let discount_ref = ctx.add_expr(ExprData::ColumnRef(discount));
    let min_discount = ctx.add_expr(ExprData::Literal(ScalarValue::Decimal128 {
        value: 5,
        precision: 15,
        scale: 2,
    }));
    let discount_above_min = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::GtEq,
        left: discount_ref,
        right: min_discount,
    });
    let discount_ref = ctx.add_expr(ExprData::ColumnRef(discount));
    let max_discount = ctx.add_expr(ExprData::Literal(ScalarValue::Decimal128 {
        value: 7,
        precision: 15,
        scale: 2,
    }));
    let discount_below_max = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::LtEq,
        left: discount_ref,
        right: max_discount,
    });
    let quantity_ref = ctx.add_expr(ExprData::ColumnRef(quantity));
    let max_quantity = ctx.add_expr(ExprData::Literal(ScalarValue::Decimal128 {
        value: 2400,
        precision: 15,
        scale: 2,
    }));
    let quantity_below_max = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Lt,
        left: quantity_ref,
        right: max_quantity,
    });
    let predicate = ctx.add_expr(ExprData::Nary {
        op: NaryOp::And,
        exprs: vec![
            shipdate_after_start,
            shipdate_before_end,
            discount_above_min,
            discount_below_max,
            quantity_below_max,
        ],
    });
    let selection = ctx.add_operator(OperatorData::Selection(Selection {
        predicate,
        input: lineitem,
    }));
    let extendedprice_ref = ctx.add_expr(ExprData::ColumnRef(extendedprice));
    let discount_ref = ctx.add_expr(ExprData::ColumnRef(discount));
    let revenue_expr = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Multiply,
        left: extendedprice_ref,
        right: discount_ref,
    });
    let aggregation = ctx.add_operator(OperatorData::Aggregation(Aggregation {
        keys: Vec::new(),
        aggregates: vec![(
            revenue,
            AggregateExpr::Func {
                func: AggregateFunction::Sum,
                arg: revenue_expr,
                distinct: false,
            },
        )],
        input: selection,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: aggregation }));
    ctx.set_root(output);

    ctx
}

fn tpch_q7() -> QueryContext {
    tpch_schema_only_query(
        "lineitem",
        vec![
            utf8_ty("supp_nation"),
            utf8_ty("cust_nation"),
            int64_ty("l_year"),
            decimal_ty("revenue"),
        ],
    )
}

fn tpch_q8() -> QueryContext {
    tpch_schema_only_query("orders", vec![int64_ty("o_year"), decimal_ty("mkt_share")])
}

fn tpch_q9() -> QueryContext {
    tpch_schema_only_query(
        "lineitem",
        vec![
            utf8_ty("nation"),
            int64_ty("o_year"),
            decimal_ty("sum_profit"),
        ],
    )
}

fn tpch_q10() -> QueryContext {
    tpch_schema_only_query(
        "customer",
        vec![
            int64_ty("c_custkey"),
            utf8_ty("c_name"),
            decimal_ty("revenue"),
            decimal_ty("c_acctbal"),
            utf8_ty("n_name"),
            utf8_ty("c_address"),
            utf8_ty("c_phone"),
            utf8_ty("c_comment"),
        ],
    )
}

fn tpch_q11() -> QueryContext {
    tpch_schema_only_query(
        "partsupp",
        vec![int64_ty("ps_partkey"), decimal_ty("value")],
    )
}

fn tpch_q12() -> QueryContext {
    tpch_schema_only_query(
        "lineitem",
        vec![
            utf8_ty("l_shipmode"),
            int64_ty("high_line_count"),
            int64_ty("low_line_count"),
        ],
    )
}

fn tpch_q13() -> QueryContext {
    tpch_schema_only_query("customer", vec![int64_ty("c_count"), int64_ty("custdist")])
}

fn tpch_q14() -> QueryContext {
    tpch_schema_only_query("lineitem", vec![decimal_ty("promo_revenue")])
}

fn tpch_q15() -> QueryContext {
    tpch_schema_only_query(
        "supplier",
        vec![
            int64_ty("s_suppkey"),
            utf8_ty("s_name"),
            utf8_ty("s_address"),
            utf8_ty("s_phone"),
            decimal_ty("total_revenue"),
        ],
    )
}

fn tpch_q16() -> QueryContext {
    tpch_schema_only_query(
        "partsupp",
        vec![
            utf8_ty("p_brand"),
            utf8_ty("p_type"),
            int32_ty("p_size"),
            int64_ty("supplier_cnt"),
        ],
    )
}

fn tpch_q17() -> QueryContext {
    tpch_schema_only_query("lineitem", vec![decimal_ty("avg_yearly")])
}

fn tpch_q18() -> QueryContext {
    tpch_schema_only_query(
        "orders",
        vec![
            utf8_ty("c_name"),
            int64_ty("c_custkey"),
            int64_ty("o_orderkey"),
            date_ty("o_orderdate"),
            decimal_ty("o_totalprice"),
            decimal_ty("sum_l_quantity"),
        ],
    )
}

fn tpch_q19() -> QueryContext {
    tpch_schema_only_query("lineitem", vec![decimal_ty("revenue")])
}

fn tpch_q20() -> QueryContext {
    tpch_schema_only_query("supplier", vec![utf8_ty("s_name"), utf8_ty("s_address")])
}

fn tpch_q21() -> QueryContext {
    tpch_schema_only_query("supplier", vec![utf8_ty("s_name"), int64_ty("numwait")])
}

fn tpch_q22() -> QueryContext {
    tpch_schema_only_query(
        "customer",
        vec![
            utf8_ty("cntrycode"),
            int64_ty("numcust"),
            decimal_ty("totacctbal"),
        ],
    )
}

fn tpch_schema_only_query(
    source_table: &'static str,
    outputs: Vec<(&'static str, arrow_schema::DataType)>,
) -> QueryContext {
    let mut ctx = QueryContext::new();
    let scan = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare(source_table),
        columns: Vec::new(),
    }));
    let mut output_columns = Vec::new();
    let mut computations = Vec::new();

    for (name, ty) in outputs {
        let column = ctx.add_column(ColumnData::new(name, ty.clone()));
        let expr = ctx.add_expr(ExprData::Literal(default_scalar(&ty)));
        output_columns.push(column);
        computations.push((column, expr));
    }

    let map = ctx.add_operator(OperatorData::Map(Map {
        computations,
        input: scan,
    }));
    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns: output_columns,
        input: map,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);

    ctx
}

fn default_scalar(ty: &arrow_schema::DataType) -> ScalarValue {
    match ty {
        arrow_schema::DataType::Int32 => ScalarValue::Int32(0),
        arrow_schema::DataType::Int64 => ScalarValue::Int64(0),
        arrow_schema::DataType::Decimal128(precision, scale) => ScalarValue::Decimal128 {
            value: 0,
            precision: *precision,
            scale: *scale,
        },
        arrow_schema::DataType::Date32 => ScalarValue::Date32(0),
        arrow_schema::DataType::Utf8 => ScalarValue::Utf8(String::new()),
        _ => ScalarValue::Null(ty.clone()),
    }
}

fn int32_ty(name: &'static str) -> (&'static str, arrow_schema::DataType) {
    (name, arrow_schema::DataType::Int32)
}

fn int64_ty(name: &'static str) -> (&'static str, arrow_schema::DataType) {
    (name, arrow_schema::DataType::Int64)
}

fn utf8_ty(name: &'static str) -> (&'static str, arrow_schema::DataType) {
    (name, arrow_schema::DataType::Utf8)
}

fn decimal_ty(name: &'static str) -> (&'static str, arrow_schema::DataType) {
    (name, arrow_schema::DataType::Decimal128(15, 2))
}

fn date_ty(name: &'static str) -> (&'static str, arrow_schema::DataType) {
    (name, arrow_schema::DataType::Date32)
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
    Field::new(name, DataFusionDataType::Int32, false)
}

fn int64(name: &'static str) -> Field {
    Field::new(name, DataFusionDataType::Int64, false)
}

fn utf8(name: &'static str) -> Field {
    Field::new(name, DataFusionDataType::Utf8, true)
}

fn decimal(name: &'static str) -> Field {
    Field::new(name, DataFusionDataType::Decimal128(15, 2), false)
}

fn date(name: &'static str) -> Field {
    Field::new(name, DataFusionDataType::Date32, false)
}
