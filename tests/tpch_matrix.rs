use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::datatypes::{DataType as DataFusionDataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use prost::Message;
use simple_graph::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Column, ColumnData, CrossProduct,
    Expr, ExprData, Join, JoinType, Limit, Map, NaryOp, NullOrdering, Operator, OperatorData,
    Output, Projection, QueryContext, ScalarFunction, ScalarValue, Scan, Selection, Sort,
    SortDirection, SortKey, TableRef, substrait,
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
    let mut ctx = QueryContext::new();
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_returnflag",
            "l_linestatus",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_shipdate",
        ],
    );

    let shipdate = col(&mut ctx, lineitem.col("l_shipdate"));
    let cutoff = date_lit(&mut ctx, 10524);
    let predicate = bin(&mut ctx, BinaryOp::LtEq, shipdate, cutoff);
    let lineitem = select_rel(&mut ctx, lineitem, predicate);

    let discounted = disc_price(
        &mut ctx,
        lineitem.col("l_extendedprice"),
        lineitem.col("l_discount"),
    );
    let one = dec_lit(&mut ctx, 100);
    let tax = col(&mut ctx, lineitem.col("l_tax"));
    let charge_factor = bin(&mut ctx, BinaryOp::Add, one, tax);
    let charge = bin(&mut ctx, BinaryOp::Multiply, discounted, charge_factor);
    let lineitem = map_rel(
        &mut ctx,
        lineitem,
        vec![
            (
                "disc_price",
                arrow_schema::DataType::Decimal128(15, 2),
                discounted,
            ),
            ("charge", arrow_schema::DataType::Decimal128(15, 2), charge),
        ],
    );

    let quantity = col(&mut ctx, lineitem.col("l_quantity"));
    let extendedprice = col(&mut ctx, lineitem.col("l_extendedprice"));
    let disc_price = col(&mut ctx, lineitem.col("disc_price"));
    let charge = col(&mut ctx, lineitem.col("charge"));
    let quantity_for_avg = col(&mut ctx, lineitem.col("l_quantity"));
    let extendedprice_for_avg = col(&mut ctx, lineitem.col("l_extendedprice"));
    let discount_for_avg = col(&mut ctx, lineitem.col("l_discount"));
    let count_arg = col(&mut ctx, lineitem.col("l_returnflag"));
    let grouped = aggregate_rel(
        &mut ctx,
        lineitem,
        &["l_returnflag", "l_linestatus"],
        vec![
            (
                "sum_qty",
                arrow_schema::DataType::Decimal128(15, 2),
                sum_expr(quantity),
            ),
            (
                "sum_base_price",
                arrow_schema::DataType::Decimal128(15, 2),
                sum_expr(extendedprice),
            ),
            (
                "sum_disc_price",
                arrow_schema::DataType::Decimal128(15, 2),
                sum_expr(disc_price),
            ),
            (
                "sum_charge",
                arrow_schema::DataType::Decimal128(15, 2),
                sum_expr(charge),
            ),
            (
                "avg_qty",
                arrow_schema::DataType::Decimal128(15, 2),
                avg_expr(quantity_for_avg),
            ),
            (
                "avg_price",
                arrow_schema::DataType::Decimal128(15, 2),
                avg_expr(extendedprice_for_avg),
            ),
            (
                "avg_disc",
                arrow_schema::DataType::Decimal128(15, 2),
                avg_expr(discount_for_avg),
            ),
            (
                "count_order",
                arrow_schema::DataType::Int64,
                count_expr(count_arg),
            ),
        ],
    );
    let grouped = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("l_returnflag", SortDirection::Asc),
            ("l_linestatus", SortDirection::Asc),
        ],
    );
    finish(
        &mut ctx,
        grouped,
        &[
            "l_returnflag",
            "l_linestatus",
            "sum_qty",
            "sum_base_price",
            "sum_disc_price",
            "sum_charge",
            "avg_qty",
            "avg_price",
            "avg_disc",
            "count_order",
        ],
    );

    ctx
}

fn tpch_q2() -> QueryContext {
    let mut ctx = QueryContext::new();
    let part = scan_rel(
        &mut ctx,
        "part",
        &["p_partkey", "p_mfgr", "p_size", "p_type"],
    );
    let supplier = scan_rel(
        &mut ctx,
        "supplier",
        &[
            "s_suppkey",
            "s_name",
            "s_address",
            "s_phone",
            "s_comment",
            "s_acctbal",
            "s_nationkey",
        ],
    );
    let partsupp = scan_rel(
        &mut ctx,
        "partsupp",
        &["ps_partkey", "ps_suppkey", "ps_supplycost"],
    );
    let nation = scan_rel(
        &mut ctx,
        "nation",
        &["n_nationkey", "n_regionkey", "n_name"],
    );
    let region = scan_rel(&mut ctx, "region", &["r_regionkey", "r_name"]);
    let joined = cross_join_on_cols(&mut ctx, part, partsupp, "p_partkey", "ps_partkey");
    let joined = cross_join_on_cols(&mut ctx, joined, supplier, "ps_suppkey", "s_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "s_nationkey", "n_nationkey");
    let joined = cross_join_on_cols(&mut ctx, joined, region, "n_regionkey", "r_regionkey");
    let region_filtered = filter_eq_str(&mut ctx, joined, "r_name", "ASIA");
    let size = col(&mut ctx, region_filtered.col("p_size"));
    let wanted_size = int_lit(&mut ctx, 48);
    let size_predicate = eq(&mut ctx, size, wanted_size);
    let part_type = col(&mut ctx, region_filtered.col("p_type"));
    let type_predicate = like(&mut ctx, part_type, "%TIN");
    let predicate = and(&mut ctx, vec![size_predicate, type_predicate]);
    let joined = select_rel(&mut ctx, region_filtered.clone(), predicate);

    let sub_partsupp = scan_rel_as(
        &mut ctx,
        "partsupp",
        Some("ps2"),
        &["ps_partkey", "ps_suppkey", "ps_supplycost"],
    );
    let sub_supplier = scan_rel_as(
        &mut ctx,
        "supplier",
        Some("s2"),
        &["s_suppkey", "s_nationkey"],
    );
    let sub_nation = scan_rel_as(
        &mut ctx,
        "nation",
        Some("n2"),
        &["n_nationkey", "n_regionkey"],
    );
    let sub_region = scan_rel_as(&mut ctx, "region", Some("r2"), &["r_regionkey", "r_name"]);
    let subquery = cross_join_on_cols(
        &mut ctx,
        sub_partsupp,
        sub_supplier,
        "ps2.ps_suppkey",
        "s2.s_suppkey",
    );
    let subquery = cross_join_on_cols(
        &mut ctx,
        subquery,
        sub_nation,
        "s2.s_nationkey",
        "n2.n_nationkey",
    );
    let subquery = cross_join_on_cols(
        &mut ctx,
        subquery,
        sub_region,
        "n2.n_regionkey",
        "r2.r_regionkey",
    );
    let subquery = filter_eq_str(&mut ctx, subquery, "r2.r_name", "ASIA");
    let outer_partkey = col(&mut ctx, joined.col("p_partkey"));
    let inner_partkey = col(&mut ctx, subquery.col("ps2.ps_partkey"));
    let correlated = eq(&mut ctx, outer_partkey, inner_partkey);
    let subquery = select_rel(&mut ctx, subquery, correlated);
    let supplycost = col(&mut ctx, subquery.col("ps2.ps_supplycost"));
    let min_cost = aggregate_rel(
        &mut ctx,
        subquery,
        &[],
        vec![(
            "min_supplycost",
            arrow_schema::DataType::Decimal128(15, 2),
            min_expr(supplycost),
        )],
    );
    let supplycost = col(&mut ctx, joined.col("ps_supplycost"));
    let min_cost = project_rel(&mut ctx, min_cost, &["min_supplycost"]);
    let min_supplycost = scalar_subquery(&mut ctx, min_cost.input);
    let predicate = eq(&mut ctx, supplycost, min_supplycost);
    let joined = select_rel(&mut ctx, joined, predicate);
    let sorted = sort_rel(
        &mut ctx,
        joined,
        vec![
            ("s_acctbal", SortDirection::Desc),
            ("n_name", SortDirection::Asc),
            ("s_name", SortDirection::Asc),
            ("p_partkey", SortDirection::Asc),
        ],
    );
    let limited = limit_rel(&mut ctx, sorted, 100);
    finish(
        &mut ctx,
        limited,
        &[
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ],
    );
    ctx
}

fn tpch_q3() -> QueryContext {
    let mut ctx = QueryContext::new();
    let customer = scan_rel(&mut ctx, "customer", &["c_custkey", "c_mktsegment"]);
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"],
    );
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
    );
    let joined = cross_join_on_cols(&mut ctx, customer, orders, "c_custkey", "o_custkey");
    let joined = cross_join_on_cols(&mut ctx, joined, lineitem, "o_orderkey", "l_orderkey");
    let joined = filter_eq_str(&mut ctx, joined, "c_mktsegment", "BUILDING");
    let orderdate = col(&mut ctx, joined.col("o_orderdate"));
    let cutoff = date_lit(&mut ctx, 9204);
    let order_before = bin(&mut ctx, BinaryOp::Lt, orderdate, cutoff);
    let shipdate = col(&mut ctx, joined.col("l_shipdate"));
    let cutoff = date_lit(&mut ctx, 9204);
    let ship_after = bin(&mut ctx, BinaryOp::Gt, shipdate, cutoff);
    let predicate = and(&mut ctx, vec![order_before, ship_after]);
    let joined = select_rel(&mut ctx, joined, predicate);
    let revenue = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![(
            "revenue_expr",
            arrow_schema::DataType::Decimal128(15, 2),
            revenue,
        )],
    );
    let revenue_arg = col(&mut ctx, joined.col("revenue_expr"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["l_orderkey", "o_orderdate", "o_shippriority"],
        vec![(
            "revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(revenue_arg),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("revenue", SortDirection::Desc),
            ("o_orderdate", SortDirection::Asc),
        ],
    );
    let limited = limit_rel(&mut ctx, sorted, 10);
    finish(
        &mut ctx,
        limited,
        &["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
    );
    ctx
}

fn tpch_q4() -> QueryContext {
    let mut ctx = QueryContext::new();
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_orderkey", "o_orderdate", "o_orderpriority"],
    );
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &["l_orderkey", "l_commitdate", "l_receiptdate"],
    );
    let commit = col(&mut ctx, lineitem.col("l_commitdate"));
    let receipt = col(&mut ctx, lineitem.col("l_receiptdate"));
    let late_lineitem = bin(&mut ctx, BinaryOp::Lt, commit, receipt);
    let line_orderkey = col(&mut ctx, lineitem.col("l_orderkey"));
    let orderkey = col(&mut ctx, orders.col("o_orderkey"));
    let correlated = eq(&mut ctx, line_orderkey, orderkey);
    let subquery_predicate = and(&mut ctx, vec![late_lineitem, correlated]);
    let lineitem = select_rel(&mut ctx, lineitem, subquery_predicate);
    let predicate = exists(&mut ctx, lineitem.input);
    let orders = select_rel(&mut ctx, orders, predicate);
    let orders = filter_date_range(&mut ctx, orders, "o_orderdate", 9221, 9312);
    let priority = col(&mut ctx, orders.col("o_orderpriority"));
    let grouped = aggregate_rel(
        &mut ctx,
        orders,
        &["o_orderpriority"],
        vec![(
            "order_count",
            arrow_schema::DataType::Int64,
            count_expr(priority),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![("o_orderpriority", SortDirection::Asc)],
    );
    finish(&mut ctx, sorted, &["o_orderpriority", "order_count"]);
    ctx
}

fn tpch_q5() -> QueryContext {
    let mut ctx = QueryContext::new();
    let customer = scan_rel(&mut ctx, "customer", &["c_custkey", "c_nationkey"]);
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_orderkey", "o_custkey", "o_orderdate"],
    );
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"],
    );
    let supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_nationkey"]);
    let nation = scan_rel(
        &mut ctx,
        "nation",
        &["n_nationkey", "n_regionkey", "n_name"],
    );
    let region = scan_rel(&mut ctx, "region", &["r_regionkey", "r_name"]);
    let joined = cross_join_on_cols(&mut ctx, customer, orders, "c_custkey", "o_custkey");
    let joined = cross_join_on_cols(&mut ctx, joined, lineitem, "o_orderkey", "l_orderkey");
    let joined = cross_join_on_cols(&mut ctx, joined, supplier, "l_suppkey", "s_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "s_nationkey", "n_nationkey");
    let joined = cross_join_on_cols(&mut ctx, joined, region, "n_regionkey", "r_regionkey");
    let joined = filter_eq_str(&mut ctx, joined, "r_name", "AFRICA");
    let joined = filter_date_range(&mut ctx, joined, "o_orderdate", 8766, 9131);
    let revenue = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![(
            "revenue_expr",
            arrow_schema::DataType::Decimal128(15, 2),
            revenue,
        )],
    );
    let revenue = col(&mut ctx, joined.col("revenue_expr"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["n_name"],
        vec![(
            "revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(revenue),
        )],
    );
    let sorted = sort_rel(&mut ctx, grouped, vec![("revenue", SortDirection::Desc)]);
    finish(&mut ctx, sorted, &["n_name", "revenue"]);
    ctx
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
    let mut ctx = QueryContext::new();
    let supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_nationkey"]);
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_suppkey",
            "l_orderkey",
            "l_shipdate",
            "l_extendedprice",
            "l_discount",
        ],
    );
    let orders = scan_rel(&mut ctx, "orders", &["o_orderkey", "o_custkey"]);
    let customer = scan_rel(&mut ctx, "customer", &["c_custkey", "c_nationkey"]);
    let n1 = scan_rel_as(&mut ctx, "nation", Some("n1"), &["n_nationkey", "n_name"]);
    let n2 = scan_rel_as(&mut ctx, "nation", Some("n2"), &["n_nationkey", "n_name"]);
    let joined = cross_join_on_cols(&mut ctx, supplier, lineitem, "s_suppkey", "l_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, orders, "l_orderkey", "o_orderkey");
    let joined = cross_join_on_cols(&mut ctx, joined, customer, "o_custkey", "c_custkey");
    let joined = cross_join_on_cols(&mut ctx, joined, n1, "s_nationkey", "n1.n_nationkey");
    let joined = cross_join_on_cols(&mut ctx, joined, n2, "c_nationkey", "n2.n_nationkey");
    let n1_name = col(&mut ctx, joined.col("n1.n_name"));
    let germany = str_lit(&mut ctx, "GERMANY");
    let n1_germany = eq(&mut ctx, n1_name, germany);
    let n2_name = col(&mut ctx, joined.col("n2.n_name"));
    let iraq = str_lit(&mut ctx, "IRAQ");
    let n2_iraq = eq(&mut ctx, n2_name, iraq);
    let germany_to_iraq = and(&mut ctx, vec![n1_germany, n2_iraq]);
    let n1_name = col(&mut ctx, joined.col("n1.n_name"));
    let iraq = str_lit(&mut ctx, "IRAQ");
    let n1_iraq = eq(&mut ctx, n1_name, iraq);
    let n2_name = col(&mut ctx, joined.col("n2.n_name"));
    let germany = str_lit(&mut ctx, "GERMANY");
    let n2_germany = eq(&mut ctx, n2_name, germany);
    let iraq_to_germany = and(&mut ctx, vec![n1_iraq, n2_germany]);
    let nation_pair = or(&mut ctx, vec![germany_to_iraq, iraq_to_germany]);
    let joined = select_rel(&mut ctx, joined, nation_pair);
    let joined = filter_date_range(&mut ctx, joined, "l_shipdate", 9131, 9862);
    let supp_nation = col(&mut ctx, joined.col("n1.n_name"));
    let cust_nation = col(&mut ctx, joined.col("n2.n_name"));
    let shipdate = col(&mut ctx, joined.col("l_shipdate"));
    let l_year = scalar_fn(&mut ctx, "extract_year", vec![shipdate]);
    let volume = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![
            ("supp_nation", arrow_schema::DataType::Utf8, supp_nation),
            ("cust_nation", arrow_schema::DataType::Utf8, cust_nation),
            ("l_year", arrow_schema::DataType::Int64, l_year),
            ("volume", arrow_schema::DataType::Decimal128(15, 2), volume),
        ],
    );
    let volume = col(&mut ctx, joined.col("volume"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["supp_nation", "cust_nation", "l_year"],
        vec![(
            "revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(volume),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("supp_nation", SortDirection::Asc),
            ("cust_nation", SortDirection::Asc),
            ("l_year", SortDirection::Asc),
        ],
    );
    finish(
        &mut ctx,
        sorted,
        &["supp_nation", "cust_nation", "l_year", "revenue"],
    );
    ctx
}

fn tpch_q8() -> QueryContext {
    let mut ctx = QueryContext::new();
    let part = scan_rel(&mut ctx, "part", &["p_partkey", "p_type"]);
    let supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_nationkey"]);
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_partkey",
            "l_suppkey",
            "l_orderkey",
            "l_extendedprice",
            "l_discount",
        ],
    );
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_orderkey", "o_custkey", "o_orderdate"],
    );
    let customer = scan_rel(&mut ctx, "customer", &["c_custkey", "c_nationkey"]);
    let n1 = scan_rel_as(
        &mut ctx,
        "nation",
        Some("n1"),
        &["n_nationkey", "n_regionkey"],
    );
    let n2 = scan_rel_as(&mut ctx, "nation", Some("n2"), &["n_nationkey", "n_name"]);
    let region = scan_rel(&mut ctx, "region", &["r_regionkey", "r_name"]);
    let joined = cross_join_on_cols(&mut ctx, part, lineitem, "p_partkey", "l_partkey");
    let joined = cross_join_on_cols(&mut ctx, joined, supplier, "l_suppkey", "s_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, orders, "l_orderkey", "o_orderkey");
    let joined = cross_join_on_cols(&mut ctx, joined, customer, "o_custkey", "c_custkey");
    let joined = cross_join_on_cols(&mut ctx, joined, n1, "c_nationkey", "n1.n_nationkey");
    let joined = cross_join_on_cols(&mut ctx, joined, region, "n1.n_regionkey", "r_regionkey");
    let joined = cross_join_on_cols(&mut ctx, joined, n2, "s_nationkey", "n2.n_nationkey");
    let joined = filter_eq_str(&mut ctx, joined, "r_name", "MIDDLE EAST");
    let joined = filter_eq_str(&mut ctx, joined, "p_type", "LARGE PLATED STEEL");
    let joined = filter_date_range(&mut ctx, joined, "o_orderdate", 9131, 9862);
    let orderdate = col(&mut ctx, joined.col("o_orderdate"));
    let o_year = scalar_fn(&mut ctx, "extract_year", vec![orderdate]);
    let volume = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let nation = col(&mut ctx, joined.col("n2.n_name"));
    let iraq = str_lit(&mut ctx, "IRAQ");
    let is_iraq = eq(&mut ctx, nation, iraq);
    let zero = dec_lit(&mut ctx, 0);
    let iraq_volume = case_when(&mut ctx, vec![(is_iraq, volume)], Some(zero));
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![
            ("o_year", arrow_schema::DataType::Int64, o_year),
            ("volume", arrow_schema::DataType::Decimal128(15, 2), volume),
            (
                "iraq_volume",
                arrow_schema::DataType::Decimal128(15, 2),
                iraq_volume,
            ),
        ],
    );
    let iraq_volume = col(&mut ctx, joined.col("iraq_volume"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["o_year"],
        vec![(
            "mkt_share",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(iraq_volume),
        )],
    );
    let sorted = sort_rel(&mut ctx, grouped, vec![("o_year", SortDirection::Asc)]);
    finish(&mut ctx, sorted, &["o_year", "mkt_share"]);
    ctx
}

fn tpch_q9() -> QueryContext {
    let mut ctx = QueryContext::new();
    let part = scan_rel(&mut ctx, "part", &["p_partkey", "p_name"]);
    let supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_nationkey"]);
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_partkey",
            "l_suppkey",
            "l_orderkey",
            "l_extendedprice",
            "l_discount",
            "l_quantity",
        ],
    );
    let partsupp = scan_rel(
        &mut ctx,
        "partsupp",
        &["ps_partkey", "ps_suppkey", "ps_supplycost"],
    );
    let orders = scan_rel(&mut ctx, "orders", &["o_orderkey", "o_orderdate"]);
    let nation = scan_rel(&mut ctx, "nation", &["n_nationkey", "n_name"]);
    let joined = cross_join_on_cols(&mut ctx, part, lineitem, "p_partkey", "l_partkey");
    let joined = cross_join_on_cols(&mut ctx, joined, supplier, "l_suppkey", "s_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, partsupp, "l_partkey", "ps_partkey");
    let ps_suppkey = col(&mut ctx, joined.col("ps_suppkey"));
    let l_suppkey = col(&mut ctx, joined.col("l_suppkey"));
    let same_supp = eq(&mut ctx, ps_suppkey, l_suppkey);
    let joined = select_rel(&mut ctx, joined, same_supp);
    let joined = cross_join_on_cols(&mut ctx, joined, orders, "l_orderkey", "o_orderkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "s_nationkey", "n_nationkey");
    let part_name = col(&mut ctx, joined.col("p_name"));
    let moccasin = like(&mut ctx, part_name, "%moccasin%");
    let joined = select_rel(&mut ctx, joined, moccasin);
    let orderdate = col(&mut ctx, joined.col("o_orderdate"));
    let o_year = scalar_fn(&mut ctx, "extract_year", vec![orderdate]);
    let revenue = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let supplycost = col(&mut ctx, joined.col("ps_supplycost"));
    let quantity = col(&mut ctx, joined.col("l_quantity"));
    let cost = bin(&mut ctx, BinaryOp::Multiply, supplycost, quantity);
    let amount = bin(&mut ctx, BinaryOp::Subtract, revenue, cost);
    let nation_name = col(&mut ctx, joined.col("n_name"));
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![
            ("nation", arrow_schema::DataType::Utf8, nation_name),
            ("o_year", arrow_schema::DataType::Int64, o_year),
            ("amount", arrow_schema::DataType::Decimal128(15, 2), amount),
        ],
    );
    let amount = col(&mut ctx, joined.col("amount"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["nation", "o_year"],
        vec![(
            "sum_profit",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(amount),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("nation", SortDirection::Asc),
            ("o_year", SortDirection::Desc),
        ],
    );
    finish(&mut ctx, sorted, &["nation", "o_year", "sum_profit"]);
    ctx
}

fn tpch_q10() -> QueryContext {
    let mut ctx = QueryContext::new();
    let customer = scan_rel(
        &mut ctx,
        "customer",
        &[
            "c_custkey",
            "c_name",
            "c_acctbal",
            "c_address",
            "c_phone",
            "c_comment",
            "c_nationkey",
        ],
    );
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_orderkey", "o_custkey", "o_orderdate"],
    );
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_orderkey",
            "l_extendedprice",
            "l_discount",
            "l_returnflag",
        ],
    );
    let nation = scan_rel(&mut ctx, "nation", &["n_nationkey", "n_name"]);
    let joined = cross_join_on_cols(&mut ctx, customer, orders, "c_custkey", "o_custkey");
    let joined = cross_join_on_cols(&mut ctx, joined, lineitem, "o_orderkey", "l_orderkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "c_nationkey", "n_nationkey");
    let joined = filter_date_range(&mut ctx, joined, "o_orderdate", 8582, 8674);
    let joined = filter_eq_str(&mut ctx, joined, "l_returnflag", "R");
    let revenue = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![(
            "revenue_expr",
            arrow_schema::DataType::Decimal128(15, 2),
            revenue,
        )],
    );
    let revenue = col(&mut ctx, joined.col("revenue_expr"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &[
            "c_custkey",
            "c_name",
            "c_acctbal",
            "c_phone",
            "n_name",
            "c_address",
            "c_comment",
        ],
        vec![(
            "revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(revenue),
        )],
    );
    let sorted = sort_rel(&mut ctx, grouped, vec![("revenue", SortDirection::Desc)]);
    let limited = limit_rel(&mut ctx, sorted, 20);
    finish(
        &mut ctx,
        limited,
        &[
            "c_custkey",
            "c_name",
            "revenue",
            "c_acctbal",
            "n_name",
            "c_address",
            "c_phone",
            "c_comment",
        ],
    );
    ctx
}

fn tpch_q11() -> QueryContext {
    let mut ctx = QueryContext::new();
    let partsupp = scan_rel(
        &mut ctx,
        "partsupp",
        &["ps_partkey", "ps_suppkey", "ps_supplycost", "ps_availqty"],
    );
    let supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_nationkey"]);
    let nation = scan_rel(&mut ctx, "nation", &["n_nationkey", "n_name"]);
    let joined = cross_join_on_cols(&mut ctx, partsupp, supplier, "ps_suppkey", "s_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "s_nationkey", "n_nationkey");
    let joined = filter_eq_str(&mut ctx, joined, "n_name", "ALGERIA");
    let supplycost = col(&mut ctx, joined.col("ps_supplycost"));
    let availqty = col(&mut ctx, joined.col("ps_availqty"));
    let value_expr = bin(&mut ctx, BinaryOp::Multiply, supplycost, availqty);
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![(
            "value_expr",
            arrow_schema::DataType::Decimal128(15, 2),
            value_expr,
        )],
    );
    let value_arg = col(&mut ctx, joined.col("value_expr"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined.clone(),
        &["ps_partkey"],
        vec![(
            "value",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(value_arg),
        )],
    );
    let value_arg = col(&mut ctx, joined.col("value_expr"));
    let total = aggregate_rel(
        &mut ctx,
        joined,
        &[],
        vec![(
            "total_value_threshold",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(value_arg),
        )],
    );
    let value = col(&mut ctx, grouped.col("value"));
    let threshold = scalar_subquery(&mut ctx, total.input);
    let predicate = bin(&mut ctx, BinaryOp::Gt, value, threshold);
    let grouped = select_rel(&mut ctx, grouped, predicate);
    let sorted = sort_rel(&mut ctx, grouped, vec![("value", SortDirection::Desc)]);
    finish(&mut ctx, sorted, &["ps_partkey", "value"]);
    ctx
}

fn tpch_q12() -> QueryContext {
    let mut ctx = QueryContext::new();
    let orders = scan_rel(&mut ctx, "orders", &["o_orderkey", "o_orderpriority"]);
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_orderkey",
            "l_shipmode",
            "l_commitdate",
            "l_receiptdate",
            "l_shipdate",
        ],
    );
    let joined = cross_join_on_cols(&mut ctx, orders, lineitem, "o_orderkey", "l_orderkey");
    let shipmode = col(&mut ctx, joined.col("l_shipmode"));
    let mode_predicate = in_list(&mut ctx, shipmode, &["FOB", "SHIP"]);
    let commit = col(&mut ctx, joined.col("l_commitdate"));
    let receipt = col(&mut ctx, joined.col("l_receiptdate"));
    let commit_before_receipt = bin(&mut ctx, BinaryOp::Lt, commit, receipt);
    let ship = col(&mut ctx, joined.col("l_shipdate"));
    let commit = col(&mut ctx, joined.col("l_commitdate"));
    let ship_before_commit = bin(&mut ctx, BinaryOp::Lt, ship, commit);
    let receipt = col(&mut ctx, joined.col("l_receiptdate"));
    let start = date_lit(&mut ctx, 9131);
    let receipt_after = bin(&mut ctx, BinaryOp::GtEq, receipt, start);
    let receipt = col(&mut ctx, joined.col("l_receiptdate"));
    let end = date_lit(&mut ctx, 9496);
    let receipt_before = bin(&mut ctx, BinaryOp::Lt, receipt, end);
    let predicate = and(
        &mut ctx,
        vec![
            mode_predicate,
            commit_before_receipt,
            ship_before_commit,
            receipt_after,
            receipt_before,
        ],
    );
    let joined = select_rel(&mut ctx, joined, predicate);
    let priority = col(&mut ctx, joined.col("o_orderpriority"));
    let urgent = str_lit(&mut ctx, "1-URGENT");
    let high_urgent = eq(&mut ctx, priority, urgent);
    let priority = col(&mut ctx, joined.col("o_orderpriority"));
    let high_literal = str_lit(&mut ctx, "2-HIGH");
    let high = eq(&mut ctx, priority, high_literal);
    let high_predicate = or(&mut ctx, vec![high_urgent, high]);
    let one = int_lit(&mut ctx, 1);
    let zero = int_lit(&mut ctx, 0);
    let high_case = case_when(&mut ctx, vec![(high_predicate, one)], Some(zero));
    let low_case = case_when(&mut ctx, vec![(high_predicate, zero)], Some(one));
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![
            ("high_priority", arrow_schema::DataType::Int64, high_case),
            ("low_priority", arrow_schema::DataType::Int64, low_case),
        ],
    );
    let high = col(&mut ctx, joined.col("high_priority"));
    let low = col(&mut ctx, joined.col("low_priority"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["l_shipmode"],
        vec![
            (
                "high_line_count",
                arrow_schema::DataType::Int64,
                sum_expr(high),
            ),
            (
                "low_line_count",
                arrow_schema::DataType::Int64,
                sum_expr(low),
            ),
        ],
    );
    let sorted = sort_rel(&mut ctx, grouped, vec![("l_shipmode", SortDirection::Asc)]);
    finish(
        &mut ctx,
        sorted,
        &["l_shipmode", "high_line_count", "low_line_count"],
    );
    ctx
}

fn tpch_q13() -> QueryContext {
    let mut ctx = QueryContext::new();
    let customer = scan_rel(&mut ctx, "customer", &["c_custkey"]);
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_custkey", "o_orderkey", "o_comment"],
    );
    let comment = col(&mut ctx, orders.col("o_comment"));
    let bad_comment = like(&mut ctx, comment, "%express%requests%");
    let good_comment = not(&mut ctx, bad_comment);
    let orders = select_rel(&mut ctx, orders, good_comment);
    let custkey = col(&mut ctx, customer.col("c_custkey"));
    let order_custkey = col(&mut ctx, orders.col("o_custkey"));
    let on = eq(&mut ctx, custkey, order_custkey);
    let joined = join_rel(&mut ctx, JoinType::LeftOuter, customer, orders, on);
    let orderkey = col(&mut ctx, joined.col("o_orderkey"));
    let counts = aggregate_rel(
        &mut ctx,
        joined,
        &["c_custkey"],
        vec![(
            "c_count",
            arrow_schema::DataType::Int64,
            count_expr(orderkey),
        )],
    );
    let c_count = col(&mut ctx, counts.col("c_count"));
    let grouped = aggregate_rel(
        &mut ctx,
        counts,
        &["c_count"],
        vec![(
            "custdist",
            arrow_schema::DataType::Int64,
            count_expr(c_count),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("custdist", SortDirection::Desc),
            ("c_count", SortDirection::Desc),
        ],
    );
    finish(&mut ctx, sorted, &["c_count", "custdist"]);
    ctx
}

fn tpch_q14() -> QueryContext {
    let mut ctx = QueryContext::new();
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &["l_partkey", "l_shipdate", "l_extendedprice", "l_discount"],
    );
    let part = scan_rel(&mut ctx, "part", &["p_partkey", "p_type"]);
    let joined = cross_join_on_cols(&mut ctx, lineitem, part, "l_partkey", "p_partkey");
    let joined = filter_date_range(&mut ctx, joined, "l_shipdate", 9162, 9190);
    let disc_price = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let part_type = col(&mut ctx, joined.col("p_type"));
    let promo = like(&mut ctx, part_type, "PROMO%");
    let zero = dec_lit(&mut ctx, 0);
    let promo_disc_price = case_when(&mut ctx, vec![(promo, disc_price)], Some(zero));
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![
            (
                "disc_price",
                arrow_schema::DataType::Decimal128(15, 2),
                disc_price,
            ),
            (
                "promo_disc_price",
                arrow_schema::DataType::Decimal128(15, 2),
                promo_disc_price,
            ),
        ],
    );
    let promo_disc_price = col(&mut ctx, joined.col("promo_disc_price"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &[],
        vec![(
            "promo_revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(promo_disc_price),
        )],
    );
    finish(&mut ctx, grouped, &["promo_revenue"]);
    ctx
}

fn tpch_q15() -> QueryContext {
    let mut ctx = QueryContext::new();
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &["l_suppkey", "l_extendedprice", "l_discount", "l_shipdate"],
    );
    let lineitem = filter_date_range(&mut ctx, lineitem, "l_shipdate", 9709, 9801);
    let revenue_expr = disc_price(
        &mut ctx,
        lineitem.col("l_extendedprice"),
        lineitem.col("l_discount"),
    );
    let lineitem = map_rel(
        &mut ctx,
        lineitem,
        vec![(
            "revenue_expr",
            arrow_schema::DataType::Decimal128(15, 2),
            revenue_expr,
        )],
    );
    let revenue_arg = col(&mut ctx, lineitem.col("revenue_expr"));
    let revenue = aggregate_rel(
        &mut ctx,
        lineitem,
        &["l_suppkey"],
        vec![(
            "total_revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(revenue_arg),
        )],
    );
    let max_arg = col(&mut ctx, revenue.col("total_revenue"));
    let max_revenue = aggregate_rel(
        &mut ctx,
        revenue.clone(),
        &[],
        vec![(
            "max_total_revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            max_expr(max_arg),
        )],
    );
    let supplier = scan_rel(
        &mut ctx,
        "supplier",
        &["s_suppkey", "s_name", "s_address", "s_phone"],
    );
    let joined = cross_join_on_cols(&mut ctx, supplier, revenue, "s_suppkey", "l_suppkey");
    let total = col(&mut ctx, joined.col("total_revenue"));
    let max_total = scalar_subquery(&mut ctx, max_revenue.input);
    let predicate = eq(&mut ctx, total, max_total);
    let joined = select_rel(&mut ctx, joined, predicate);
    let sorted = sort_rel(&mut ctx, joined, vec![("s_suppkey", SortDirection::Asc)]);
    finish(
        &mut ctx,
        sorted,
        &[
            "s_suppkey",
            "s_name",
            "s_address",
            "s_phone",
            "total_revenue",
        ],
    );
    ctx
}

fn tpch_q16() -> QueryContext {
    let mut ctx = QueryContext::new();
    let partsupp = scan_rel(&mut ctx, "partsupp", &["ps_partkey", "ps_suppkey"]);
    let part = scan_rel(
        &mut ctx,
        "part",
        &["p_partkey", "p_brand", "p_type", "p_size"],
    );
    let supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_comment"]);
    let joined = cross_join_on_cols(&mut ctx, partsupp, part, "ps_partkey", "p_partkey");
    let brand = col(&mut ctx, joined.col("p_brand"));
    let brand_literal = str_lit(&mut ctx, "Brand#14");
    let not_brand = bin(&mut ctx, BinaryOp::NotEq, brand, brand_literal);
    let part_type = col(&mut ctx, joined.col("p_type"));
    let plated = like(&mut ctx, part_type, "SMALL PLATED%");
    let not_plated = not(&mut ctx, plated);
    let size = col(&mut ctx, joined.col("p_size"));
    let size_14 = int_lit(&mut ctx, 14);
    let size_6 = int_lit(&mut ctx, 6);
    let size_5 = int_lit(&mut ctx, 5);
    let size_31 = int_lit(&mut ctx, 31);
    let size_49 = int_lit(&mut ctx, 49);
    let size_15 = int_lit(&mut ctx, 15);
    let size_41 = int_lit(&mut ctx, 41);
    let size_47 = int_lit(&mut ctx, 47);
    let size_ok = scalar_fn(
        &mut ctx,
        "in",
        vec![
            size, size_14, size_6, size_5, size_31, size_49, size_15, size_41, size_47,
        ],
    );
    let predicate = and(&mut ctx, vec![not_brand, not_plated, size_ok]);
    let joined = select_rel(&mut ctx, joined, predicate);
    let comment = col(&mut ctx, supplier.col("s_comment"));
    let complaints = like(&mut ctx, comment, "%Customer%Complaints%");
    let supplier = select_rel(&mut ctx, supplier, complaints);
    let ps_suppkey = col(&mut ctx, joined.col("ps_suppkey"));
    let supplier_keys = project_rel(&mut ctx, supplier, &["s_suppkey"]);
    let predicate = not_in_subquery(&mut ctx, ps_suppkey, supplier_keys.input);
    let joined = select_rel(&mut ctx, joined, predicate);
    let suppkey = col(&mut ctx, joined.col("ps_suppkey"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["p_brand", "p_type", "p_size"],
        vec![(
            "supplier_cnt",
            arrow_schema::DataType::Int64,
            count_distinct_expr(suppkey),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("supplier_cnt", SortDirection::Desc),
            ("p_brand", SortDirection::Asc),
            ("p_type", SortDirection::Asc),
            ("p_size", SortDirection::Asc),
        ],
    );
    finish(
        &mut ctx,
        sorted,
        &["p_brand", "p_type", "p_size", "supplier_cnt"],
    );
    ctx
}

fn tpch_q17() -> QueryContext {
    let mut ctx = QueryContext::new();
    let part = scan_rel(&mut ctx, "part", &["p_partkey", "p_brand", "p_container"]);
    let part = filter_eq_str(&mut ctx, part, "p_brand", "Brand#42");
    let part = filter_eq_str(&mut ctx, part, "p_container", "LG BAG");
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &["l_partkey", "l_quantity", "l_extendedprice"],
    );
    let joined = cross_join_on_cols(&mut ctx, part, lineitem, "p_partkey", "l_partkey");
    let sub_lineitem = scan_rel_as(
        &mut ctx,
        "lineitem",
        Some("l2"),
        &["l_partkey", "l_quantity"],
    );
    let sub_partkey = col(&mut ctx, sub_lineitem.col("l2.l_partkey"));
    let outer_partkey = col(&mut ctx, joined.col("p_partkey"));
    let correlated = eq(&mut ctx, sub_partkey, outer_partkey);
    let sub_lineitem = select_rel(&mut ctx, sub_lineitem, correlated);
    let avg_quantity_arg = col(&mut ctx, sub_lineitem.col("l2.l_quantity"));
    let avg_by_part = aggregate_rel(
        &mut ctx,
        sub_lineitem,
        &[],
        vec![(
            "avg_quantity",
            arrow_schema::DataType::Decimal128(15, 2),
            avg_expr(avg_quantity_arg),
        )],
    );
    let quantity = col(&mut ctx, joined.col("l_quantity"));
    let avg_by_part = project_rel(&mut ctx, avg_by_part, &["avg_quantity"]);
    let avg_quantity = scalar_subquery(&mut ctx, avg_by_part.input);
    let predicate = bin(&mut ctx, BinaryOp::Lt, quantity, avg_quantity);
    let joined = select_rel(&mut ctx, joined, predicate);
    let extendedprice = col(&mut ctx, joined.col("l_extendedprice"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &[],
        vec![(
            "avg_yearly",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(extendedprice),
        )],
    );
    finish(&mut ctx, grouped, &["avg_yearly"]);
    ctx
}

fn tpch_q18() -> QueryContext {
    let mut ctx = QueryContext::new();
    let lineitem_sub = scan_rel(&mut ctx, "lineitem", &["l_orderkey", "l_quantity"]);
    let quantity = col(&mut ctx, lineitem_sub.col("l_quantity"));
    let large_orders = aggregate_rel(
        &mut ctx,
        lineitem_sub,
        &["l_orderkey"],
        vec![(
            "sum_l_quantity_filter",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(quantity),
        )],
    );
    let sum_qty = col(&mut ctx, large_orders.col("sum_l_quantity_filter"));
    let threshold = dec_lit(&mut ctx, 31300);
    let predicate = bin(&mut ctx, BinaryOp::Gt, sum_qty, threshold);
    let large_orders = select_rel(&mut ctx, large_orders, predicate);

    let customer = scan_rel(&mut ctx, "customer", &["c_name", "c_custkey"]);
    let orders = scan_rel(
        &mut ctx,
        "orders",
        &["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"],
    );
    let lineitem = scan_rel(&mut ctx, "lineitem", &["l_orderkey", "l_quantity"]);
    let orderkey = col(&mut ctx, orders.col("o_orderkey"));
    let large_order_keys = project_rel(&mut ctx, large_orders, &["l_orderkey"]);
    let predicate = in_subquery(&mut ctx, orderkey, large_order_keys.input);
    let orders = select_rel(&mut ctx, orders, predicate);
    let joined = cross_join_on_cols(&mut ctx, customer, orders, "c_custkey", "o_custkey");
    let joined = cross_join_on_cols(&mut ctx, joined, lineitem, "o_orderkey", "l_orderkey");
    let quantity = col(&mut ctx, joined.col("l_quantity"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &[
            "c_name",
            "c_custkey",
            "o_orderkey",
            "o_orderdate",
            "o_totalprice",
        ],
        vec![(
            "sum_l_quantity",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(quantity),
        )],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("o_totalprice", SortDirection::Desc),
            ("o_orderdate", SortDirection::Asc),
        ],
    );
    let limited = limit_rel(&mut ctx, sorted, 100);
    finish(
        &mut ctx,
        limited,
        &[
            "c_name",
            "c_custkey",
            "o_orderkey",
            "o_orderdate",
            "o_totalprice",
            "sum_l_quantity",
        ],
    );
    ctx
}

fn tpch_q19() -> QueryContext {
    let mut ctx = QueryContext::new();
    let lineitem = scan_rel(
        &mut ctx,
        "lineitem",
        &[
            "l_partkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_shipmode",
            "l_shipinstruct",
        ],
    );
    let part = scan_rel(
        &mut ctx,
        "part",
        &["p_partkey", "p_brand", "p_container", "p_size"],
    );
    let joined = cross_join_on_cols(&mut ctx, lineitem, part, "l_partkey", "p_partkey");
    let shipmode = col(&mut ctx, joined.col("l_shipmode"));
    let air = in_list(&mut ctx, shipmode, &["AIR", "AIR REG"]);
    let instruct = col(&mut ctx, joined.col("l_shipinstruct"));
    let deliver_literal = str_lit(&mut ctx, "DELIVER IN PERSON");
    let deliver = eq(&mut ctx, instruct, deliver_literal);
    let base = and(&mut ctx, vec![air, deliver]);

    let brand = col(&mut ctx, joined.col("p_brand"));
    let brand_literal = str_lit(&mut ctx, "Brand#21");
    let brand_21 = eq(&mut ctx, brand, brand_literal);
    let qty = col(&mut ctx, joined.col("l_quantity"));
    let low = dec_lit(&mut ctx, 800);
    let high = dec_lit(&mut ctx, 1800);
    let qty_1 = between(&mut ctx, qty, low, high);
    let case_1 = and(&mut ctx, vec![brand_21, qty_1]);

    let brand = col(&mut ctx, joined.col("p_brand"));
    let brand_literal = str_lit(&mut ctx, "Brand#13");
    let brand_13 = eq(&mut ctx, brand, brand_literal);
    let qty = col(&mut ctx, joined.col("l_quantity"));
    let low = dec_lit(&mut ctx, 2000);
    let high = dec_lit(&mut ctx, 3000);
    let qty_2 = between(&mut ctx, qty, low, high);
    let case_2 = and(&mut ctx, vec![brand_13, qty_2]);

    let brand = col(&mut ctx, joined.col("p_brand"));
    let brand_literal = str_lit(&mut ctx, "Brand#52");
    let brand_52 = eq(&mut ctx, brand, brand_literal);
    let qty = col(&mut ctx, joined.col("l_quantity"));
    let low = dec_lit(&mut ctx, 3000);
    let high = dec_lit(&mut ctx, 4000);
    let qty_3 = between(&mut ctx, qty, low, high);
    let case_3 = and(&mut ctx, vec![brand_52, qty_3]);
    let brand_cases = or(&mut ctx, vec![case_1, case_2, case_3]);
    let predicate = and(&mut ctx, vec![base, brand_cases]);
    let joined = select_rel(&mut ctx, joined, predicate);
    let revenue = disc_price(
        &mut ctx,
        joined.col("l_extendedprice"),
        joined.col("l_discount"),
    );
    let joined = map_rel(
        &mut ctx,
        joined,
        vec![(
            "revenue_expr",
            arrow_schema::DataType::Decimal128(15, 2),
            revenue,
        )],
    );
    let revenue = col(&mut ctx, joined.col("revenue_expr"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &[],
        vec![(
            "revenue",
            arrow_schema::DataType::Decimal128(15, 2),
            sum_expr(revenue),
        )],
    );
    finish(&mut ctx, grouped, &["revenue"]);
    ctx
}

fn tpch_q20() -> QueryContext {
    let mut ctx = QueryContext::new();
    let part = scan_rel(&mut ctx, "part", &["p_partkey", "p_name"]);
    let part_name = col(&mut ctx, part.col("p_name"));
    let blanched = like(&mut ctx, part_name, "blanched%");
    let part = select_rel(&mut ctx, part, blanched);
    let partsupp = scan_rel(
        &mut ctx,
        "partsupp",
        &["ps_partkey", "ps_suppkey", "ps_availqty"],
    );
    let partkey = col(&mut ctx, partsupp.col("ps_partkey"));
    let part_keys = project_rel(&mut ctx, part, &["p_partkey"]);
    let partkey_in = in_subquery(&mut ctx, partkey, part_keys.input);
    let candidate_partsupp = select_rel(&mut ctx, partsupp, partkey_in);
    let supplier = scan_rel(
        &mut ctx,
        "supplier",
        &["s_suppkey", "s_name", "s_address", "s_nationkey"],
    );
    let nation = scan_rel(&mut ctx, "nation", &["n_nationkey", "n_name"]);
    let suppkey = col(&mut ctx, supplier.col("s_suppkey"));
    let partsupp_keys = project_rel(&mut ctx, candidate_partsupp, &["ps_suppkey"]);
    let suppkey_in = in_subquery(&mut ctx, suppkey, partsupp_keys.input);
    let supplier = select_rel(&mut ctx, supplier, suppkey_in);
    let joined = cross_join_on_cols(&mut ctx, supplier, nation, "s_nationkey", "n_nationkey");
    let joined = filter_eq_str(&mut ctx, joined, "n_name", "KENYA");
    let sorted = sort_rel(&mut ctx, joined, vec![("s_name", SortDirection::Asc)]);
    finish(&mut ctx, sorted, &["s_name", "s_address"]);
    ctx
}

fn tpch_q21() -> QueryContext {
    let mut ctx = QueryContext::new();
    let supplier = scan_rel(
        &mut ctx,
        "supplier",
        &["s_suppkey", "s_name", "s_nationkey"],
    );
    let lineitem = scan_rel_as(
        &mut ctx,
        "lineitem",
        Some("l1"),
        &["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"],
    );
    let orders = scan_rel(&mut ctx, "orders", &["o_orderkey", "o_orderstatus"]);
    let nation = scan_rel(&mut ctx, "nation", &["n_nationkey", "n_name"]);
    let joined = cross_join_on_cols(&mut ctx, supplier, lineitem, "s_suppkey", "l1.l_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, orders, "l1.l_orderkey", "o_orderkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "s_nationkey", "n_nationkey");
    let joined = filter_eq_str(&mut ctx, joined, "o_orderstatus", "F");
    let joined = filter_eq_str(&mut ctx, joined, "n_name", "ARGENTINA");
    let receipt = col(&mut ctx, joined.col("l1.l_receiptdate"));
    let commit = col(&mut ctx, joined.col("l1.l_commitdate"));
    let late = bin(&mut ctx, BinaryOp::Gt, receipt, commit);
    let joined = select_rel(&mut ctx, joined, late);

    let l2 = scan_rel_as(
        &mut ctx,
        "lineitem",
        Some("l2"),
        &["l_orderkey", "l_suppkey"],
    );
    let orderkey = col(&mut ctx, joined.col("l1.l_orderkey"));
    let l2_orderkey = col(&mut ctx, l2.col("l2.l_orderkey"));
    let same_order = eq(&mut ctx, orderkey, l2_orderkey);
    let suppkey = col(&mut ctx, joined.col("l1.l_suppkey"));
    let l2_suppkey = col(&mut ctx, l2.col("l2.l_suppkey"));
    let different_supplier = bin(&mut ctx, BinaryOp::NotEq, suppkey, l2_suppkey);
    let predicate = and(&mut ctx, vec![same_order, different_supplier]);
    let l2 = select_rel(&mut ctx, l2, predicate);
    let predicate = exists(&mut ctx, l2.input);
    let joined = select_rel(&mut ctx, joined, predicate);

    let l3 = scan_rel_as(
        &mut ctx,
        "lineitem",
        Some("l3"),
        &["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"],
    );
    let receipt = col(&mut ctx, l3.col("l3.l_receiptdate"));
    let commit = col(&mut ctx, l3.col("l3.l_commitdate"));
    let late = bin(&mut ctx, BinaryOp::Gt, receipt, commit);
    let l3 = select_rel(&mut ctx, l3, late);
    let orderkey = col(&mut ctx, joined.col("l1.l_orderkey"));
    let l3_orderkey = col(&mut ctx, l3.col("l3.l_orderkey"));
    let same_order = eq(&mut ctx, orderkey, l3_orderkey);
    let suppkey = col(&mut ctx, joined.col("l1.l_suppkey"));
    let l3_suppkey = col(&mut ctx, l3.col("l3.l_suppkey"));
    let different_supplier = bin(&mut ctx, BinaryOp::NotEq, suppkey, l3_suppkey);
    let predicate = and(&mut ctx, vec![same_order, different_supplier]);
    let l3 = select_rel(&mut ctx, l3, predicate);
    let predicate = not_exists(&mut ctx, l3.input);
    let joined = select_rel(&mut ctx, joined, predicate);
    let name = col(&mut ctx, joined.col("s_name"));
    let grouped = aggregate_rel(
        &mut ctx,
        joined,
        &["s_name"],
        vec![("numwait", arrow_schema::DataType::Int64, count_expr(name))],
    );
    let sorted = sort_rel(
        &mut ctx,
        grouped,
        vec![
            ("numwait", SortDirection::Desc),
            ("s_name", SortDirection::Asc),
        ],
    );
    let limited = limit_rel(&mut ctx, sorted, 100);
    finish(&mut ctx, limited, &["s_name", "numwait"]);
    ctx
}

fn tpch_q22() -> QueryContext {
    let mut ctx = QueryContext::new();
    let customer = scan_rel(&mut ctx, "customer", &["c_custkey", "c_phone", "c_acctbal"]);
    let phone = col(&mut ctx, customer.col("c_phone"));
    let start = int_lit(&mut ctx, 1);
    let length = int_lit(&mut ctx, 2);
    let cntrycode = scalar_fn(&mut ctx, "substring", vec![phone, start, length]);
    let customer = map_rel(
        &mut ctx,
        customer,
        vec![("cntrycode", arrow_schema::DataType::Utf8, cntrycode)],
    );
    let code = col(&mut ctx, customer.col("cntrycode"));
    let code_ok = in_list(&mut ctx, code, &["24", "34", "16", "30", "33", "14", "13"]);
    let acctbal = col(&mut ctx, customer.col("c_acctbal"));
    let zero = dec_lit(&mut ctx, 0);
    let positive = bin(&mut ctx, BinaryOp::Gt, acctbal, zero);
    let predicate = and(&mut ctx, vec![code_ok, positive]);
    let customer = select_rel(&mut ctx, customer, predicate);
    let acctbal = col(&mut ctx, customer.col("c_acctbal"));
    let avg_bal = aggregate_rel(
        &mut ctx,
        customer.clone(),
        &[],
        vec![(
            "avg_acctbal",
            arrow_schema::DataType::Decimal128(15, 2),
            avg_expr(acctbal),
        )],
    );
    let acctbal = col(&mut ctx, customer.col("c_acctbal"));
    let avg = scalar_subquery(&mut ctx, avg_bal.input);
    let above_avg = bin(&mut ctx, BinaryOp::Gt, acctbal, avg);
    let customer = select_rel(&mut ctx, customer, above_avg);
    let orders = scan_rel(&mut ctx, "orders", &["o_custkey"]);
    let custkey = col(&mut ctx, customer.col("c_custkey"));
    let order_custkey = col(&mut ctx, orders.col("o_custkey"));
    let predicate = eq(&mut ctx, custkey, order_custkey);
    let orders = select_rel(&mut ctx, orders, predicate);
    let predicate = not_exists(&mut ctx, orders.input);
    let customer = select_rel(&mut ctx, customer, predicate);
    let custkey = col(&mut ctx, customer.col("c_custkey"));
    let acctbal = col(&mut ctx, customer.col("c_acctbal"));
    let grouped = aggregate_rel(
        &mut ctx,
        customer,
        &["cntrycode"],
        vec![
            (
                "numcust",
                arrow_schema::DataType::Int64,
                count_expr(custkey),
            ),
            (
                "totacctbal",
                arrow_schema::DataType::Decimal128(15, 2),
                sum_expr(acctbal),
            ),
        ],
    );
    let sorted = sort_rel(&mut ctx, grouped, vec![("cntrycode", SortDirection::Asc)]);
    finish(&mut ctx, sorted, &["cntrycode", "numcust", "totacctbal"]);
    ctx
}

#[derive(Clone)]
struct Rel {
    input: Operator,
    cols: HashMap<String, Column>,
}

impl Rel {
    fn col(&self, name: &str) -> Column {
        *self
            .cols
            .get(name)
            .unwrap_or_else(|| panic!("missing column in test TPC-H builder: {name}"))
    }

    fn merge(mut self, other: Rel) -> Rel {
        self.cols.extend(other.cols);
        self
    }
}

fn scan_rel(ctx: &mut QueryContext, table: &'static str, columns: &[&'static str]) -> Rel {
    scan_rel_as(ctx, table, None, columns)
}

fn scan_rel_as(
    ctx: &mut QueryContext,
    table: &'static str,
    alias: Option<&'static str>,
    columns: &[&'static str],
) -> Rel {
    let mut cols = HashMap::new();
    let mut scan_columns = Vec::new();

    for name in columns {
        let display_name = alias
            .map(|alias| format!("{alias}.{name}"))
            .unwrap_or_else(|| (*name).to_string());
        let column = ctx.add_column(ColumnData::new(display_name, tpch_column_type(name)));
        cols.insert((*name).to_string(), column);
        if let Some(alias) = alias {
            cols.insert(format!("{alias}.{name}"), column);
        }
        scan_columns.push(column);
    }

    let input = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare(table),
        columns: scan_columns,
    }));
    Rel { input, cols }
}

fn cross_rel(ctx: &mut QueryContext, left: Rel, right: Rel) -> Rel {
    let input = ctx.add_operator(OperatorData::CrossProduct(CrossProduct {
        outer: left.input,
        inner: right.input,
    }));
    Rel {
        input,
        cols: left.merge(right).cols,
    }
}

fn join_rel(ctx: &mut QueryContext, join_type: JoinType, left: Rel, right: Rel, on: Expr) -> Rel {
    let input = ctx.add_operator(OperatorData::Join(Join {
        join_type: join_type.clone(),
        on,
        outer: left.input,
        inner: right.input,
    }));
    let cols = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => left.cols,
        _ => left.merge(right).cols,
    };
    Rel { input, cols }
}

fn cross_join_on_cols(
    ctx: &mut QueryContext,
    left: Rel,
    right: Rel,
    left_col: &str,
    right_col: &str,
) -> Rel {
    let joined = cross_rel(ctx, left, right);
    let left = col(ctx, joined.col(left_col));
    let right = col(ctx, joined.col(right_col));
    let predicate = eq(ctx, left, right);
    select_rel(ctx, joined, predicate)
}

fn filter_eq_str(ctx: &mut QueryContext, rel: Rel, column: &str, value: &'static str) -> Rel {
    let left = col(ctx, rel.col(column));
    let right = str_lit(ctx, value);
    let predicate = eq(ctx, left, right);
    select_rel(ctx, rel, predicate)
}

fn filter_date_range(
    ctx: &mut QueryContext,
    rel: Rel,
    column: &str,
    start_days: i32,
    end_days: i32,
) -> Rel {
    let value = col(ctx, rel.col(column));
    let start = date_lit(ctx, start_days);
    let after_start = bin(ctx, BinaryOp::GtEq, value, start);
    let value = col(ctx, rel.col(column));
    let end = date_lit(ctx, end_days);
    let before_end = bin(ctx, BinaryOp::Lt, value, end);
    let predicate = and(ctx, vec![after_start, before_end]);
    select_rel(ctx, rel, predicate)
}

fn select_rel(ctx: &mut QueryContext, rel: Rel, predicate: Expr) -> Rel {
    let input = ctx.add_operator(OperatorData::Selection(Selection {
        predicate,
        input: rel.input,
    }));
    Rel {
        input,
        cols: rel.cols,
    }
}

fn map_rel(
    ctx: &mut QueryContext,
    rel: Rel,
    computations: Vec<(&'static str, arrow_schema::DataType, Expr)>,
) -> Rel {
    let mut cols = rel.cols;
    let mut map_computations = Vec::new();

    for (name, ty, expr) in computations {
        let column = ctx.add_column(ColumnData::new(name, ty));
        cols.insert(name.to_string(), column);
        map_computations.push((column, expr));
    }

    let input = ctx.add_operator(OperatorData::Map(Map {
        computations: map_computations,
        input: rel.input,
    }));
    Rel { input, cols }
}

fn aggregate_rel(
    ctx: &mut QueryContext,
    rel: Rel,
    key_columns: &[&str],
    aggregates: Vec<(&'static str, arrow_schema::DataType, AggregateExpr)>,
) -> Rel {
    let mut cols = HashMap::new();
    let keys = key_columns
        .iter()
        .map(|name| {
            let column = rel.col(name);
            cols.insert((*name).to_string(), column);
            col(ctx, column)
        })
        .collect();
    let aggregates = aggregates
        .into_iter()
        .map(|(name, ty, expr)| {
            let column = ctx.add_column(ColumnData::new(name, ty));
            cols.insert(name.to_string(), column);
            (column, expr)
        })
        .collect();

    let input = ctx.add_operator(OperatorData::Aggregation(Aggregation {
        keys,
        aggregates,
        input: rel.input,
    }));
    Rel { input, cols }
}

fn sort_rel(ctx: &mut QueryContext, rel: Rel, keys: Vec<(&str, SortDirection)>) -> Rel {
    let sort_keys = keys
        .into_iter()
        .map(|(name, direction)| SortKey {
            expr: col(ctx, rel.col(name)),
            direction,
            nulls: NullOrdering::Last,
        })
        .collect();
    let input = ctx.add_operator(OperatorData::Sort(Sort {
        keys: sort_keys,
        input: rel.input,
    }));
    Rel {
        input,
        cols: rel.cols,
    }
}

fn limit_rel(ctx: &mut QueryContext, rel: Rel, fetch: usize) -> Rel {
    let input = ctx.add_operator(OperatorData::Limit(Limit {
        fetch: Some(fetch),
        offset: 0,
        input: rel.input,
    }));
    Rel {
        input,
        cols: rel.cols,
    }
}

fn project_rel(ctx: &mut QueryContext, rel: Rel, output_columns: &[&str]) -> Rel {
    let columns = output_columns
        .iter()
        .map(|name| rel.col(name))
        .collect::<Vec<_>>();
    let input = ctx.add_operator(OperatorData::Projection(Projection {
        columns: columns.clone(),
        input: rel.input,
    }));
    let cols = output_columns
        .iter()
        .zip(columns)
        .map(|(name, column)| ((*name).to_string(), column))
        .collect();
    Rel { input, cols }
}

fn finish(ctx: &mut QueryContext, rel: Rel, output_columns: &[&str]) {
    let columns = output_columns.iter().map(|name| rel.col(name)).collect();
    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns,
        input: rel.input,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);
}

fn col(ctx: &mut QueryContext, column: Column) -> Expr {
    ctx.add_expr(ExprData::ColumnRef(column))
}

fn lit(ctx: &mut QueryContext, value: ScalarValue) -> Expr {
    ctx.add_expr(ExprData::Literal(value))
}

fn int_lit(ctx: &mut QueryContext, value: i64) -> Expr {
    lit(ctx, ScalarValue::Int64(value))
}

fn dec_lit(ctx: &mut QueryContext, value: i128) -> Expr {
    lit(
        ctx,
        ScalarValue::Decimal128 {
            value,
            precision: 15,
            scale: 2,
        },
    )
}

fn str_lit(ctx: &mut QueryContext, value: &'static str) -> Expr {
    lit(ctx, ScalarValue::Utf8(value.to_string()))
}

fn date_lit(ctx: &mut QueryContext, days_since_epoch: i32) -> Expr {
    lit(ctx, ScalarValue::Date32(days_since_epoch))
}

fn bin(ctx: &mut QueryContext, op: BinaryOp, left: Expr, right: Expr) -> Expr {
    ctx.add_expr(ExprData::Binary { op, left, right })
}

fn eq(ctx: &mut QueryContext, left: Expr, right: Expr) -> Expr {
    bin(ctx, BinaryOp::Eq, left, right)
}

fn and(ctx: &mut QueryContext, exprs: Vec<Expr>) -> Expr {
    ctx.add_expr(ExprData::Nary {
        op: NaryOp::And,
        exprs,
    })
}

fn or(ctx: &mut QueryContext, exprs: Vec<Expr>) -> Expr {
    ctx.add_expr(ExprData::Nary {
        op: NaryOp::Or,
        exprs,
    })
}

fn not(ctx: &mut QueryContext, expr: Expr) -> Expr {
    ctx.add_expr(ExprData::Unary {
        op: simple_graph::UnaryOp::Not,
        expr,
    })
}

fn scalar_fn(ctx: &mut QueryContext, name: &'static str, args: Vec<Expr>) -> Expr {
    ctx.add_expr(ExprData::ScalarFunction {
        function: ScalarFunction::extension(name),
        args,
    })
}

fn exists(ctx: &mut QueryContext, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::Exists {
        subquery,
        negated: false,
    })
}

fn not_exists(ctx: &mut QueryContext, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::Exists {
        subquery,
        negated: true,
    })
}

fn in_subquery(ctx: &mut QueryContext, expr: Expr, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::InSubquery {
        expr,
        subquery,
        negated: false,
    })
}

fn not_in_subquery(ctx: &mut QueryContext, expr: Expr, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::InSubquery {
        expr,
        subquery,
        negated: true,
    })
}

fn scalar_subquery(ctx: &mut QueryContext, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::ScalarSubquery { subquery })
}

fn like(ctx: &mut QueryContext, value: Expr, pattern: &'static str) -> Expr {
    let pattern = str_lit(ctx, pattern);
    scalar_fn(ctx, "like", vec![value, pattern])
}

fn in_list(ctx: &mut QueryContext, value: Expr, values: &[&'static str]) -> Expr {
    let mut args = vec![value];
    for value in values {
        args.push(str_lit(ctx, value));
    }
    scalar_fn(ctx, "in", args)
}

fn between(ctx: &mut QueryContext, value: Expr, low: Expr, high: Expr) -> Expr {
    let above = bin(ctx, BinaryOp::GtEq, value, low);
    let below = bin(ctx, BinaryOp::LtEq, value, high);
    and(ctx, vec![above, below])
}

fn disc_price(ctx: &mut QueryContext, extendedprice: Column, discount: Column) -> Expr {
    let one = dec_lit(ctx, 100);
    let discount = col(ctx, discount);
    let keep = bin(ctx, BinaryOp::Subtract, one, discount);
    let extendedprice = col(ctx, extendedprice);
    bin(ctx, BinaryOp::Multiply, extendedprice, keep)
}

fn case_when(
    ctx: &mut QueryContext,
    when_then: Vec<(Expr, Expr)>,
    else_expr: Option<Expr>,
) -> Expr {
    ctx.add_expr(ExprData::CaseWhen {
        when_then,
        else_expr,
    })
}

fn sum_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Sum,
        arg,
        distinct: false,
    }
}

fn count_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Count,
        arg,
        distinct: false,
    }
}

fn count_distinct_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Count,
        arg,
        distinct: true,
    }
}

fn avg_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Avg,
        arg,
        distinct: false,
    }
}

fn min_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Min,
        arg,
        distinct: false,
    }
}

fn max_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Max,
        arg,
        distinct: false,
    }
}

fn tpch_column_type(name: &str) -> arrow_schema::DataType {
    match name {
        "l_linenumber" | "p_size" | "ps_availqty" | "o_shippriority" => {
            arrow_schema::DataType::Int32
        }
        "c_custkey" | "c_nationkey" | "l_orderkey" | "l_partkey" | "l_suppkey" | "n_nationkey"
        | "n_regionkey" | "o_orderkey" | "o_custkey" | "p_partkey" | "ps_partkey"
        | "ps_suppkey" | "r_regionkey" | "s_suppkey" | "s_nationkey" => {
            arrow_schema::DataType::Int64
        }
        "c_acctbal" | "l_quantity" | "l_extendedprice" | "l_discount" | "l_tax"
        | "o_totalprice" | "p_retailprice" | "ps_supplycost" | "s_acctbal" => {
            arrow_schema::DataType::Decimal128(15, 2)
        }
        "l_shipdate" | "l_commitdate" | "l_receiptdate" | "o_orderdate" => {
            arrow_schema::DataType::Date32
        }
        _ => arrow_schema::DataType::Utf8,
    }
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
