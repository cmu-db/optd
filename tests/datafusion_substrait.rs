use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use prost::Message;
use simple_graph::{
    AggregateExpr, Aggregation, ColumnData, CrossProduct, ExprData, Limit, Map, NullOrdering,
    OperatorData, Output, Projection, QueryContext, ScalarValue, Scan, Selection, Sort,
    SortDirection, SortKey, TableRef, substrait,
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

#[tokio::test]
async fn datafusion_consumes_semi_and_anti_join_plans_produced_by_simple_graph() {
    let df_ctx = users_orders_session_context();

    for join_type in [
        simple_graph::JoinType::LeftSemi,
        simple_graph::JoinType::LeftAnti,
    ] {
        let query = semi_or_anti_joined_users_orders_query(join_type);
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
}

#[tokio::test]
async fn datafusion_consumes_cross_substrait_plan_produced_by_simple_graph() {
    let df_ctx = users_orders_session_context();
    let query = crossed_users_orders_query();
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

#[tokio::test]
async fn datafusion_consumes_aggregation_substrait_plan_produced_by_simple_graph() {
    let df_ctx = users_session_context();
    let query = aggregated_users_query();
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
    assert_eq!(fields[1].name(), "user_count");
}

#[tokio::test]
async fn datafusion_consumes_map_substrait_plan_produced_by_simple_graph() {
    let df_ctx = users_session_context();
    let query = mapped_users_query();
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
    assert_eq!(fields[1].name(), "id_plus_one");
}

#[tokio::test]
async fn datafusion_consumes_cast_and_case_when_plan_produced_by_simple_graph() {
    let df_ctx = users_session_context();
    let query = cast_and_case_when_users_query();
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
    assert_eq!(fields[0].name(), "age_i32");
    assert_eq!(fields[0].data_type(), &DataType::Int32);
    assert_eq!(fields[1].name(), "age_bucket");
    assert_eq!(fields[1].data_type(), &DataType::Utf8);
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
    let id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);

    let scan = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![id, age],
    })
    .add(&mut ctx);
    let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
    let selection = OperatorData::Selection(Selection {
        predicate,
        input: scan,
    })
    .add(&mut ctx);
    let id_ref = ExprData::ColumnRef(id).add(&mut ctx);
    let sort = OperatorData::Sort(Sort {
        keys: vec![SortKey {
            expr: id_ref,
            direction: SortDirection::Desc,
            nulls: NullOrdering::Last,
        }],
        input: selection,
    })
    .add(&mut ctx);
    let limit = OperatorData::Limit(Limit {
        fetch: Some(5),
        offset: 0,
        input: sort,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![id],
        input: limit,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn joined_users_orders_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);
    let order_id = ColumnData::new("order_id", arrow_schema::DataType::Int64).add(&mut ctx);
    let order_user_id = ColumnData::new("user_id", arrow_schema::DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    })
    .add(&mut ctx);
    let orders = OperatorData::Scan(Scan {
        table: TableRef::bare("orders"),
        columns: vec![order_id, order_user_id],
    })
    .add(&mut ctx);
    let left = ExprData::ColumnRef(user_id).add(&mut ctx);
    let right = ExprData::ColumnRef(order_user_id).add(&mut ctx);
    let predicate = ExprData::Binary {
        op: simple_graph::BinaryOp::Eq,
        left,
        right,
    }
    .add(&mut ctx);
    let join = OperatorData::Join(simple_graph::Join {
        join_type: simple_graph::JoinType::Inner,
        on: predicate,
        outer: users,
        inner: orders,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![user_id, order_id],
        input: join,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn semi_or_anti_joined_users_orders_query(join_type: simple_graph::JoinType) -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);
    let order_id = ColumnData::new("order_id", arrow_schema::DataType::Int64).add(&mut ctx);
    let order_user_id = ColumnData::new("user_id", arrow_schema::DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    })
    .add(&mut ctx);
    let orders = OperatorData::Scan(Scan {
        table: TableRef::bare("orders"),
        columns: vec![order_id, order_user_id],
    })
    .add(&mut ctx);
    let left = ExprData::ColumnRef(user_id).add(&mut ctx);
    let right = ExprData::ColumnRef(order_user_id).add(&mut ctx);
    let predicate = ExprData::Binary {
        op: simple_graph::BinaryOp::Eq,
        left,
        right,
    }
    .add(&mut ctx);
    let join = OperatorData::Join(simple_graph::Join {
        join_type,
        on: predicate,
        outer: users,
        inner: orders,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![user_id],
        input: join,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn crossed_users_orders_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);
    let order_id = ColumnData::new("order_id", arrow_schema::DataType::Int64).add(&mut ctx);
    let order_user_id = ColumnData::new("user_id", arrow_schema::DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    })
    .add(&mut ctx);
    let orders = OperatorData::Scan(Scan {
        table: TableRef::bare("orders"),
        columns: vec![order_id, order_user_id],
    })
    .add(&mut ctx);
    let cross = OperatorData::CrossProduct(CrossProduct {
        outer: users,
        inner: orders,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![user_id, order_id],
        input: cross,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn aggregated_users_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_count = ColumnData::new("user_count", arrow_schema::DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    })
    .add(&mut ctx);
    let key = ExprData::ColumnRef(user_id).add(&mut ctx);
    let aggregation = OperatorData::Aggregation(Aggregation {
        keys: vec![key],
        aggregates: vec![(user_count, AggregateExpr::CountStar)],
        input: users,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: aggregation }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn mapped_users_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);
    let id_plus_one = ColumnData::new("id_plus_one", arrow_schema::DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    })
    .add(&mut ctx);
    let id_ref = ExprData::ColumnRef(user_id).add(&mut ctx);
    let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
    let add = ExprData::Binary {
        op: simple_graph::BinaryOp::Add,
        left: id_ref,
        right: one,
    }
    .add(&mut ctx);
    let map = OperatorData::Map(Map {
        computations: vec![(id_plus_one, add)],
        input: users,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![user_id, id_plus_one],
        input: map,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn cast_and_case_when_users_query() -> QueryContext {
    let mut ctx = QueryContext::new();
    let user_id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("age", arrow_schema::DataType::Int64).add(&mut ctx);
    let age_i32 = ColumnData::new("age_i32", arrow_schema::DataType::Int32).add(&mut ctx);
    let age_bucket = ColumnData::new("age_bucket", arrow_schema::DataType::Utf8).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age],
    })
    .add(&mut ctx);
    let age_ref_for_cast = ExprData::ColumnRef(user_age).add(&mut ctx);
    let cast = ExprData::Cast {
        expr: age_ref_for_cast,
        ty: arrow_schema::DataType::Int32,
    }
    .add(&mut ctx);

    let age_ref_for_condition = ExprData::ColumnRef(user_age).add(&mut ctx);
    let adult_age = ExprData::Literal(ScalarValue::Int64(18)).add(&mut ctx);
    let is_adult = ExprData::Binary {
        op: simple_graph::BinaryOp::GtEq,
        left: age_ref_for_condition,
        right: adult_age,
    }
    .add(&mut ctx);
    let adult = ExprData::Literal(ScalarValue::Utf8("adult".to_string())).add(&mut ctx);
    let minor = ExprData::Literal(ScalarValue::Utf8("minor".to_string())).add(&mut ctx);
    let bucket = ExprData::CaseWhen {
        when_then: vec![(is_adult, adult)],
        else_expr: Some(minor),
    }
    .add(&mut ctx);

    let map = OperatorData::Map(Map {
        computations: vec![(age_i32, cast), (age_bucket, bucket)],
        input: users,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![age_i32, age_bucket],
        input: map,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}
