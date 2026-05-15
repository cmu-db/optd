use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::new_empty_array;
use datafusion::arrow::datatypes::{DataType as DataFusionDataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use prost::Message;
use simple_graph::tpch::*;
use simple_graph::{
    AggregateExpr, Expr, ExprData, NaryOp, Operator, OperatorData, QueryContext, substrait,
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

#[test]
fn tpch_q3_models_where_clause_as_one_selection() {
    let query = tpch_q3();
    let mut predicates = Vec::new();
    collect_selection_predicates(
        &query,
        query.root().expect("Q3 builder should set a root"),
        &mut predicates,
    );

    assert_eq!(
        predicates.len(),
        1,
        "Q3 should keep its WHERE clause in one Selection"
    );
    let ExprData::Nary {
        op: NaryOp::And,
        exprs,
    } = query.expr(predicates[0])
    else {
        panic!("Q3 WHERE predicate should be a conjunction");
    };
    assert_eq!(
        exprs.len(),
        5,
        "Q3 WHERE clause should include the two joins and three filters"
    );
}

#[test]
fn tpch_builders_keep_one_selection_per_query_block_filter() {
    let expected_selection_counts = [
        (1, 1),
        (2, 2),
        (3, 1),
        (4, 2),
        (5, 1),
        (6, 1),
        (7, 1),
        (8, 1),
        (9, 1),
        (10, 1),
        (11, 2),
        (12, 1),
        (13, 0),
        (14, 1),
        (15, 2),
        (16, 2),
        (17, 2),
        (18, 2),
        (19, 1),
        (20, 4),
        (21, 3),
        (22, 3),
    ];

    for (query_number, expected) in expected_selection_counts {
        let query = tpch_query(query_number).expect("TPC-H builder should exist");
        let mut selections = Vec::new();
        let mut visited = HashSet::new();
        collect_unique_selection_predicates(
            &query,
            query.root().expect("TPC-H builder should set a root"),
            &mut visited,
            &mut selections,
        );

        assert_eq!(
            selections.len(),
            expected,
            "Q{query_number} should model each query-block filter with one Selection"
        );
    }
}

fn collect_selection_predicates(
    query: &QueryContext,
    operator: Operator,
    predicates: &mut Vec<simple_graph::Expr>,
) {
    match query.operator(operator) {
        OperatorData::Scan(_) | OperatorData::TableFunction(_) => {}
        OperatorData::Selection(selection) => {
            predicates.push(selection.predicate);
            collect_selection_predicates(query, selection.input, predicates);
        }
        OperatorData::Map(map) => collect_selection_predicates(query, map.input, predicates),
        OperatorData::Sort(sort) => collect_selection_predicates(query, sort.input, predicates),
        OperatorData::Limit(limit) => collect_selection_predicates(query, limit.input, predicates),
        OperatorData::Aggregation(aggregation) => {
            collect_selection_predicates(query, aggregation.input, predicates);
        }
        OperatorData::Projection(projection) => {
            collect_selection_predicates(query, projection.input, predicates);
        }
        OperatorData::Output(output) => {
            collect_selection_predicates(query, output.input, predicates)
        }
        OperatorData::CrossProduct(cross) => {
            collect_selection_predicates(query, cross.outer, predicates);
            collect_selection_predicates(query, cross.inner, predicates);
        }
        OperatorData::Join(join) => {
            collect_selection_predicates(query, join.outer, predicates);
            collect_selection_predicates(query, join.inner, predicates);
        }
        OperatorData::Rename(r) => collect_selection_predicates(query, r.input, predicates),
    }
}

fn collect_unique_selection_predicates(
    query: &QueryContext,
    operator: Operator,
    visited: &mut HashSet<Operator>,
    predicates: &mut Vec<simple_graph::Expr>,
) {
    if !visited.insert(operator) {
        return;
    }

    match query.operator(operator) {
        OperatorData::Scan(_) | OperatorData::TableFunction(_) => {}
        OperatorData::Selection(selection) => {
            predicates.push(selection.predicate);
            collect_expr_subquery_selections(query, selection.predicate, visited, predicates);
            collect_unique_selection_predicates(query, selection.input, visited, predicates);
        }
        OperatorData::Map(map) => {
            for (_, expr) in &map.computations {
                collect_expr_subquery_selections(query, *expr, visited, predicates);
            }
            collect_unique_selection_predicates(query, map.input, visited, predicates);
        }
        OperatorData::Sort(sort) => {
            for key in &sort.keys {
                collect_expr_subquery_selections(query, key.expr, visited, predicates);
            }
            collect_unique_selection_predicates(query, sort.input, visited, predicates);
        }
        OperatorData::Limit(limit) => {
            collect_unique_selection_predicates(query, limit.input, visited, predicates);
        }
        OperatorData::Aggregation(aggregation) => {
            for key in &aggregation.keys {
                collect_expr_subquery_selections(query, *key, visited, predicates);
            }
            for (_, aggregate) in &aggregation.aggregates {
                collect_aggregate_subquery_selections(query, aggregate, visited, predicates);
            }
            collect_unique_selection_predicates(query, aggregation.input, visited, predicates);
        }
        OperatorData::Projection(projection) => {
            collect_unique_selection_predicates(query, projection.input, visited, predicates);
        }
        OperatorData::Output(output) => {
            collect_unique_selection_predicates(query, output.input, visited, predicates)
        }
        OperatorData::CrossProduct(cross) => {
            collect_unique_selection_predicates(query, cross.outer, visited, predicates);
            collect_unique_selection_predicates(query, cross.inner, visited, predicates);
        }
        OperatorData::Join(join) => {
            collect_expr_subquery_selections(query, join.on, visited, predicates);
            collect_unique_selection_predicates(query, join.outer, visited, predicates);
            collect_unique_selection_predicates(query, join.inner, visited, predicates);
        }
        OperatorData::Rename(r) => {
            collect_unique_selection_predicates(query, r.input, visited, predicates);
        }
    }
}

fn collect_aggregate_subquery_selections(
    query: &QueryContext,
    aggregate: &AggregateExpr,
    visited: &mut HashSet<Operator>,
    predicates: &mut Vec<simple_graph::Expr>,
) {
    match aggregate {
        AggregateExpr::CountStar => {}
        AggregateExpr::Func { arg, .. } => {
            collect_expr_subquery_selections(query, *arg, visited, predicates);
        }
    }
}

fn collect_expr_subquery_selections(
    query: &QueryContext,
    expr: Expr,
    visited: &mut HashSet<Operator>,
    predicates: &mut Vec<simple_graph::Expr>,
) {
    match query.expr(expr) {
        ExprData::Literal(_) | ExprData::ColumnRef(_) => {}
        ExprData::Unary { expr, .. } | ExprData::Cast { expr, .. } => {
            collect_expr_subquery_selections(query, *expr, visited, predicates);
        }
        ExprData::Binary { left, right, .. } => {
            collect_expr_subquery_selections(query, *left, visited, predicates);
            collect_expr_subquery_selections(query, *right, visited, predicates);
        }
        ExprData::Nary { exprs, .. } | ExprData::ScalarFunction { args: exprs, .. } => {
            for expr in exprs {
                collect_expr_subquery_selections(query, *expr, visited, predicates);
            }
        }
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                collect_expr_subquery_selections(query, *when, visited, predicates);
                collect_expr_subquery_selections(query, *then, visited, predicates);
            }
            if let Some(else_expr) = else_expr {
                collect_expr_subquery_selections(query, *else_expr, visited, predicates);
            }
        }
        ExprData::Exists { subquery, .. } | ExprData::ScalarSubquery { subquery } => {
            collect_unique_selection_predicates(query, *subquery, visited, predicates);
        }
        ExprData::InSubquery { expr, subquery, .. } => {
            collect_expr_subquery_selections(query, *expr, visited, predicates);
            collect_unique_selection_predicates(query, *subquery, visited, predicates);
        }
        ExprData::Like { expr, pattern, .. } => {
            collect_expr_subquery_selections(query, *expr, visited, predicates);
            collect_expr_subquery_selections(query, *pattern, visited, predicates);
        }
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
