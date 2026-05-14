use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use prost::Message;
use simple_graph::{
    ColumnData, ExprData, Limit, NullOrdering, OperatorData, Output, Projection, QueryContext,
    ScalarValue, Scan, Selection, Sort, SortDirection, SortKey, TableRef, substrait,
};

#[tokio::test]
async fn imports_substrait_plan_produced_by_datafusion() {
    let df_ctx = users_session_context();

    let bytes = datafusion_substrait::serializer::serialize_bytes(
        "SELECT id FROM users WHERE age >= 18 ORDER BY id DESC LIMIT 5",
        &df_ctx,
    )
    .await
    .unwrap();
    let plan = ::substrait::proto::Plan::decode(bytes.as_slice()).unwrap();

    let query = substrait::from_plan(&plan).unwrap();
    let root = query.root().unwrap();
    assert!(matches!(query.operator(root), OperatorData::Output(_)));

    let rendered = query.pretty_flat();
    assert!(rendered.contains("\"kind\": \"scan\""), "{rendered}");
    assert!(rendered.contains("\"kind\": \"selection\""), "{rendered}");
    assert!(rendered.contains("\"kind\": \"sort\""), "{rendered}");
    assert!(rendered.contains("\"kind\": \"limit\""), "{rendered}");
    assert!(rendered.contains("\"kind\": \"projection\""), "{rendered}");

    assert!(rendered.contains("Desc"), "{rendered}");
}

#[tokio::test]
async fn datafusion_consumes_substrait_plan_produced_by_simple_graph() {
    let df_ctx = users_session_context();
    let query = projected_users_query();
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

    let fields = logical_plan.schema().fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "id");
}

#[tokio::test]
async fn datafusion_consumes_join_substrait_plan_produced_by_simple_graph() {
    let df_ctx = users_orders_session_context();
    let query = joined_users_orders_query();
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

    let fields = logical_plan.schema().fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "id");
    assert_eq!(fields[1].name(), "order_id");
}

fn users_session_context() -> SessionContext {
    let df_ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("age", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(17), Some(18), None])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    df_ctx.register_table("users", Arc::new(table)).unwrap();
    df_ctx
}

fn users_orders_session_context() -> SessionContext {
    let df_ctx = users_session_context();
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Int64Array::from(vec![1, 2, 4])),
        ],
    )
    .unwrap();
    let orders = MemTable::try_new(orders_schema, vec![vec![orders_batch]]).unwrap();
    df_ctx.register_table("orders", Arc::new(orders)).unwrap();
    df_ctx
}

fn projected_users_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let id = ctx.add_column(ColumnData::new("id", arrow_schema::DataType::Int64));
    let age = ctx.add_column(ColumnData::new("age", arrow_schema::DataType::Int64));

    let scan = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![id, age],
    }));
    let predicate = ctx.add_expr(ExprData::Literal(ScalarValue::Boolean(true)));
    let selection = ctx.add_operator(OperatorData::Selection(Selection {
        predicate,
        input: scan,
    }));
    let id_ref = ctx.add_expr(ExprData::ColumnRef(id));
    let sort = ctx.add_operator(OperatorData::Sort(Sort {
        keys: vec![SortKey {
            expr: id_ref,
            direction: SortDirection::Desc,
            nulls: NullOrdering::Last,
        }],
        input: selection,
    }));
    let limit = ctx.add_operator(OperatorData::Limit(Limit {
        fetch: Some(5),
        offset: 0,
        input: sort,
    }));
    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns: vec![id],
        input: limit,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);

    ctx
}

fn joined_users_orders_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ctx.add_column(ColumnData::new("id", arrow_schema::DataType::Int64));
    let user_age = ctx.add_column(ColumnData::new("age", arrow_schema::DataType::Int64));
    let order_id = ctx.add_column(ColumnData::new("order_id", arrow_schema::DataType::Int64));
    let order_user_id = ctx.add_column(ColumnData::new("user_id", arrow_schema::DataType::Int64));

    let users = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    }));
    let orders = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("orders"),
        columns: vec![order_id, order_user_id],
    }));
    let left = ctx.add_expr(ExprData::ColumnRef(user_id));
    let right = ctx.add_expr(ExprData::ColumnRef(order_user_id));
    let predicate = ctx.add_expr(ExprData::Binary {
        op: simple_graph::BinaryOp::Eq,
        left,
        right,
    });
    let join = ctx.add_operator(OperatorData::Join(simple_graph::Join {
        join_type: simple_graph::JoinType::Inner,
        on: predicate,
        outer: users,
        inner: orders,
    }));
    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns: vec![user_id, order_id],
        input: join,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);

    ctx
}
