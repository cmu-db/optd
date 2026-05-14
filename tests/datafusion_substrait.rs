use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use prost::Message;
use simple_graph::{OperatorData, substrait};

#[tokio::test]
async fn imports_substrait_plan_produced_by_datafusion() {
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
