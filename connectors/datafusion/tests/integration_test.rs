use datafusion::{
    arrow::{
        array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    },
    catalog::{CatalogProviderList, MemorySchemaProvider, TableProvider},
    datasource::MemTable,
    execution::context::SessionContext,
    prelude::*,
};
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_datafusion::{OptdCatalogProvider, OptdCatalogProviderList, OptdTableProvider};
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn create_test_catalog() -> (TempDir, DuckLakeCatalog) {
    let temp_dir = TempDir::new().unwrap();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_dir = temp_dir
        .path()
        .join(format!("df_test_{}_{}", timestamp, counter));
    std::fs::create_dir_all(&unique_dir).unwrap();

    let db_path = unique_dir.join("test.db");
    let metadata_path = unique_dir.join("metadata.ducklake");

    let catalog = DuckLakeCatalog::try_new(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )
    .unwrap();

    (temp_dir, catalog)
}

fn create_test_data(
    fields: Vec<(&str, DataType)>,
    columns: Vec<Arc<dyn datafusion::arrow::array::Array>>,
) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, dtype)| Field::new(name, dtype, false))
            .collect::<Vec<Field>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    (schema, batch)
}

async fn get_wrapped_catalog(
    catalog_list: Arc<dyn CatalogProviderList>,
    catalog_handle: Option<optd_catalog::CatalogServiceHandle>,
) -> Arc<OptdCatalogProvider> {
    let optd_catalog_list = OptdCatalogProviderList::new(catalog_list, catalog_handle);
    let catalog = optd_catalog_list.catalog("datafusion").unwrap();
    Arc::new(
        catalog
            .as_any()
            .downcast_ref::<OptdCatalogProvider>()
            .unwrap()
            .clone(),
    )
}

async fn get_wrapped_table(
    catalog_list: Arc<dyn CatalogProviderList>,
    catalog_handle: Option<optd_catalog::CatalogServiceHandle>,
    table_name: &str,
) -> Arc<OptdTableProvider> {
    let optd_catalog_list = OptdCatalogProviderList::new(catalog_list, catalog_handle);
    let catalog = optd_catalog_list.catalog("datafusion").unwrap();
    let schema = catalog.schema("public").unwrap();
    let table = schema
        .table(table_name)
        .await
        .expect("Failed to retrieve table")
        .expect("Table not found");
    Arc::new(
        table
            .as_any()
            .downcast_ref::<OptdTableProvider>()
            .unwrap()
            .clone(),
    )
}

#[tokio::test]
async fn test_catalog_provider_list_wrapping() {
    let ctx = SessionContext::new();
    let catalog_list = ctx.state().catalog_list().clone();

    let optd_catalog_list = OptdCatalogProviderList::new(catalog_list.clone(), None);

    let original_names = catalog_list.catalog_names();
    let wrapped_names = optd_catalog_list.catalog_names();
    assert_eq!(original_names, wrapped_names);
    assert!(wrapped_names.contains(&"datafusion".to_string()));
}

#[tokio::test]
async fn test_table_provider_wrapping() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
    let optd_table = OptdTableProvider::new(mem_table.clone(), "test_table".to_string());

    assert_eq!(optd_table.table_name(), "test_table");
    assert_eq!(optd_table.schema(), schema);
    assert!(optd_table.statistics().is_none());
}

#[tokio::test]
async fn test_schema_retrieval() {
    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![("id", DataType::Int32), ("value", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    );
    ctx.register_batch("numbers", batch).unwrap();

    let optd_table = get_wrapped_table(ctx.state().catalog_list().clone(), None, "numbers").await;
    assert_eq!(optd_table.table_name(), "numbers");

    let schema = optd_table.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(1).name(), "value");
    assert_eq!(schema.field(1).data_type(), &DataType::Int32);

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    assert_eq!(schema.as_ref(), expected_schema.as_ref());
}

#[tokio::test]
async fn test_query_execution_with_wrapped_catalog() {
    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![("id", DataType::Int32), ("value", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    );
    ctx.register_batch("test_data", batch).unwrap();

    let results = ctx
        .sql("SELECT id, value FROM test_data WHERE value > 20")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);
    assert_eq!(
        results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[3, 4, 5]
    );
}

#[tokio::test]
async fn test_table_provider_accessibility_from_plan() {
    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    );
    ctx.register_batch("users", batch).unwrap();

    let df = ctx.sql("SELECT * FROM users").await.unwrap();
    assert!(format!("{:?}", df.logical_plan()).contains("users"));

    let results = df.collect().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);
    assert_eq!(
        results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
    assert_eq!(
        results[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("Alice"), Some("Bob")]
    );
}

#[tokio::test]
async fn test_table_metadata_access_through_catalog() {
    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![
            ("customer_id", DataType::Int32),
            ("order_amount", DataType::Int32),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1, 3, 2, 1])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250, 120])),
        ],
    );
    ctx.register_batch("orders", batch).unwrap();

    let optd_table = get_wrapped_table(ctx.state().catalog_list().clone(), None, "orders").await;
    let catalog = get_wrapped_catalog(ctx.state().catalog_list().clone(), None).await;

    assert_eq!(optd_table.table_name(), "orders");
    assert!(catalog.catalog_handle().is_none());
    assert!(optd_table.statistics().is_none());

    let results = ctx
        .sql("SELECT customer_id, SUM(order_amount) FROM orders GROUP BY customer_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 rows for 3 unique customers");
    assert!(!results.is_empty(), "Should have at least one batch");
    assert_eq!(
        results[0].num_columns(),
        2,
        "Each batch should have 2 columns (customer_id and sum)"
    );

    // Collect all results into vectors for verification
    let mut all_customer_ids = Vec::new();
    let mut all_sums = Vec::new();
    for batch in &results {
        let customer_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        all_customer_ids.extend(customer_ids.values());
        all_sums.extend(sums.values());
    }

    // Sort by customer_id for consistent verification
    let mut pairs: Vec<_> = all_customer_ids
        .iter()
        .zip(all_sums.iter())
        .map(|(c, s)| (*c, *s))
        .collect();
    pairs.sort_by_key(|p| p.0);

    assert_eq!(
        pairs,
        vec![(1, 370), (2, 450), (3, 300)],
        "Expected customer_id 1->370, 2->450, 3->300"
    );
}

#[tokio::test]
async fn test_csv_table_wrapping() {
    let _tmp_dir = tempfile::TempDir::new().unwrap();
    let csv_path = _tmp_dir.path().join("test.csv");
    let mut file = std::fs::File::create(&csv_path).unwrap();
    std::io::Write::write_all(&mut file, b"id,value\n1,10\n2,20\n").unwrap();

    let ctx = SessionContext::new();

    ctx.register_csv(
        "test_csv",
        csv_path.to_str().unwrap(),
        CsvReadOptions::default(),
    )
    .await
    .unwrap();

    let df = ctx.sql("SELECT * FROM test_csv").await.unwrap();
    let results = df.collect().await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // CSV columns are typically parsed as Int64, not Int32
    let id_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let value_col = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.values(), &[1, 2]);
    assert_eq!(value_col.values(), &[10, 20]);
}

#[tokio::test]
async fn test_full_optimizer_integration_pipeline() {
    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![
            ("product_id", DataType::Int32),
            ("category", DataType::Utf8),
            ("price", DataType::Int32),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "B"])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250])),
        ],
    );
    ctx.register_batch("products", batch).unwrap();

    let catalog_list = ctx.state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(catalog_list, None);
    let catalog = optd_catalog_list.catalog("datafusion").unwrap();
    assert!(catalog.schema_names().contains(&"public".to_string()));

    let df = ctx
        .sql("SELECT category, AVG(price) as avg_price FROM products GROUP BY category")
        .await
        .unwrap();

    assert!(format!("{:?}", df.logical_plan()).contains("products"));

    let results = df.collect().await.unwrap();
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 categories");
    assert_eq!(results[0].num_columns(), 2);

    // Collect and verify exact AVG results: A->125, B->225, C->300
    let mut category_avgs = Vec::new();
    for batch in &results {
        let categories = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let avg_prices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            category_avgs.push((categories.value(i).to_string(), avg_prices.value(i)));
        }
    }
    category_avgs.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(category_avgs.len(), 3);
    assert_eq!(category_avgs[0].0, "A");
    assert!(
        (category_avgs[0].1 - 125.0).abs() < 0.01,
        "Category A avg should be 125"
    );
    assert_eq!(category_avgs[1].0, "B");
    assert!(
        (category_avgs[1].1 - 225.0).abs() < 0.01,
        "Category B avg should be 225"
    );
    assert_eq!(category_avgs[2].0, "C");
    assert!(
        (category_avgs[2].1 - 300.0).abs() < 0.01,
        "Category C avg should be 300"
    );
}

// Tests with CatalogService integration

#[tokio::test]
async fn test_catalog_service_handle_propagation() {
    let (_temp_dir, catalog) = create_test_catalog();
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let ctx = SessionContext::new();
    let (schema, batch) = create_test_data(
        vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    );
    ctx.register_batch("users", batch).unwrap();

    let optd_table = get_wrapped_table(
        ctx.state().catalog_list().clone(),
        Some(handle.clone()),
        "users",
    )
    .await;
    let catalog = get_wrapped_catalog(ctx.state().catalog_list().clone(), Some(handle)).await;

    assert!(catalog.catalog_handle().is_some());
    assert_eq!(optd_table.table_name(), "users");
    assert_eq!(optd_table.schema(), schema);
}

#[tokio::test]
async fn test_catalog_service_snapshot_retrieval() {
    let (_temp_dir, catalog) = create_test_catalog();
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![("id", DataType::Int32)],
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    );
    ctx.register_batch("test", batch).unwrap();

    let catalog = get_wrapped_catalog(ctx.state().catalog_list().clone(), Some(handle)).await;
    let catalog_handle = catalog.catalog_handle().unwrap();

    let snapshot = catalog_handle.current_snapshot().await.unwrap();
    assert_eq!(snapshot.0, 0, "Fresh catalog should start at snapshot 0");

    let snapshot_info = catalog_handle.current_snapshot_info().await.unwrap();
    assert_eq!(snapshot_info.id.0, 0);
    assert_eq!(snapshot_info.schema_version, 0);
    assert!(snapshot_info.next_catalog_id >= 0);
    assert!(snapshot_info.next_file_id >= 0);
}

#[tokio::test]
async fn test_catalog_service_schema_retrieval() {
    let (_temp_dir, catalog) = create_test_catalog();
    let conn = catalog.get_connection();
    conn.execute_batch(
        "CREATE TABLE test_schema_table (id INTEGER, value VARCHAR, amount DECIMAL(10,2))",
    )
    .unwrap();

    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let schema = handle
        .current_schema(None, "test_schema_table")
        .await
        .unwrap();

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "value");
    assert_eq!(schema.field(2).name(), "amount");
}

#[tokio::test]
async fn test_full_workflow_with_catalog_service() {
    let (_temp_dir, catalog) = create_test_catalog();
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![
            ("product_id", DataType::Int32),
            ("category", DataType::Utf8),
            ("price", DataType::Int32),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "B"])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250])),
        ],
    );
    ctx.register_batch("products", batch).unwrap();

    let catalog =
        get_wrapped_catalog(ctx.state().catalog_list().clone(), Some(handle.clone())).await;

    assert!(catalog.catalog_handle().is_some());

    let snapshot = catalog
        .catalog_handle()
        .unwrap()
        .current_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.0, 0, "Fresh catalog should start at snapshot 0");

    let results = ctx
        .sql("SELECT category, AVG(price) as avg_price FROM products GROUP BY category")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 categories");

    // Verify exact AVG results
    let mut category_avgs = Vec::new();
    for batch in &results {
        let categories = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let avg_prices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            category_avgs.push((categories.value(i).to_string(), avg_prices.value(i)));
        }
    }
    category_avgs.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        category_avgs,
        vec![
            ("A".to_string(), 125.0),
            ("B".to_string(), 225.0),
            ("C".to_string(), 300.0)
        ]
    );
}

#[tokio::test]
async fn test_catalog_service_statistics_update_and_retrieval() {
    let (_temp_dir, catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    // Create a table with known structure
    conn.execute_batch(
        "CREATE TABLE stats_table (id INTEGER, name VARCHAR, age INTEGER);
         INSERT INTO stats_table VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);",
    )
    .unwrap();

    // Get table_id and column_id for statistics
    let table_id: i64 = conn.query_row(
        "SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
         INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
         WHERE ds.schema_name = current_schema() AND dt.table_name = 'stats_table'",
        [],
        |row| row.get(0),
    ).unwrap();

    let age_column_id: i64 = conn
        .query_row(
            "SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
         WHERE table_id = ? AND column_name = 'age'",
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    // Update statistics through the catalog service
    handle
        .update_table_column_stats(age_column_id, table_id, "ndv", r#"{"distinct_count": 3}"#)
        .await
        .unwrap();

    handle
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .await
        .unwrap();

    handle
        .update_table_column_stats(age_column_id, table_id, "max_value", "35")
        .await
        .unwrap();

    // Retrieve statistics
    let snapshot = handle.current_snapshot().await.unwrap();
    let stats = handle
        .table_statistics("stats_table", snapshot)
        .await
        .unwrap();

    assert!(stats.is_some(), "Statistics should be available");
    let stats = stats.unwrap();

    // Verify table-level statistics
    assert_eq!(stats.row_count, 3, "Table should have 3 rows");

    // Verify column statistics
    let age_stats = stats
        .column_statistics
        .iter()
        .find(|c| c.name == "age")
        .expect("age column should have statistics");

    assert_eq!(
        age_stats.advanced_stats.len(),
        3,
        "Should have 3 stat types"
    );

    let ndv_stat = age_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "ndv")
        .expect("Should have ndv statistic");
    assert_eq!(ndv_stat.data, serde_json::json!({"distinct_count": 3}));

    let min_stat = age_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "min_value")
        .expect("Should have min_value statistic");
    assert_eq!(min_stat.data, serde_json::json!(25));

    let max_stat = age_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "max_value")
        .expect("Should have max_value statistic");
    assert_eq!(max_stat.data, serde_json::json!(35));
}

#[tokio::test]
async fn test_catalog_service_with_datafusion_integration() {
    let (_temp_dir, catalog) = create_test_catalog();
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let ctx = SessionContext::new();
    let (_, batch) = create_test_data(
        vec![("id", DataType::Int32), ("value", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250])),
        ],
    );
    ctx.register_batch("test_table", batch).unwrap();

    let catalog = get_wrapped_catalog(ctx.state().catalog_list().clone(), Some(handle)).await;

    let snapshot = catalog
        .catalog_handle()
        .unwrap()
        .current_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.0, 0);

    let results = ctx
        .sql("SELECT id, value FROM test_table WHERE value > 150")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    // Verify exact filtered results: rows with value > 150 are (2,200), (4,300), (5,250)
    let id_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let value_col = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col.values(), &[2, 4, 5]);
    assert_eq!(value_col.values(), &[200, 300, 250]);
}

#[tokio::test]
async fn test_multiple_schemas_isolation() {
    let ctx = SessionContext::new();

    // Register tables in the default "public" schema
    let (_, batch1) = create_test_data(
        vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    );
    ctx.register_batch("users", batch1).unwrap();

    // Create a custom schema and register a table there
    let (_, batch2) = create_test_data(
        vec![("id", DataType::Int32), ("department", DataType::Utf8)],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales"])),
        ],
    );

    // DataFusion's default catalog structure: catalog.schema.table
    // We'll use the memory catalog provider to create multiple schemas
    let mem_table = MemTable::try_new(batch2.schema(), vec![vec![batch2]]).unwrap();
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema("custom_schema", Arc::new(MemorySchemaProvider::new()))
        .unwrap();

    ctx.catalog("datafusion")
        .unwrap()
        .schema("custom_schema")
        .unwrap()
        .register_table("departments".to_string(), Arc::new(mem_table))
        .unwrap();

    // Wrap with OptdCatalogProviderList
    let catalog_list = ctx.state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(catalog_list, None);

    // Test 1: Verify both schemas exist
    let catalog = optd_catalog_list.catalog("datafusion").unwrap();
    let schema_names = catalog.schema_names();
    assert!(schema_names.contains(&"public".to_string()));
    assert!(schema_names.contains(&"custom_schema".to_string()));

    // Test 2: Verify tables are isolated in their respective schemas
    let public_schema = catalog.schema("public").unwrap();
    let custom_schema = catalog.schema("custom_schema").unwrap();

    let users_in_public = public_schema.table("users").await.unwrap();
    assert!(
        users_in_public.is_some(),
        "users should exist in public schema"
    );
    let departments_in_public = public_schema.table("departments").await.unwrap();
    assert!(
        departments_in_public.is_none(),
        "departments should not exist in public schema"
    );

    let departments_in_custom = custom_schema.table("departments").await.unwrap();
    assert!(
        departments_in_custom.is_some(),
        "departments should exist in custom_schema"
    );
    let users_in_custom = custom_schema.table("users").await.unwrap();
    assert!(
        users_in_custom.is_none(),
        "users should not exist in custom_schema"
    );

    // Test 3: Verify OptdTableProvider wraps tables from both schemas
    let users_table = users_in_public.unwrap();
    let users_optd = users_table
        .as_any()
        .downcast_ref::<OptdTableProvider>()
        .unwrap();
    assert_eq!(users_optd.table_name(), "users");

    let departments_table = departments_in_custom.unwrap();
    let departments_optd = departments_table
        .as_any()
        .downcast_ref::<OptdTableProvider>()
        .unwrap();
    assert_eq!(departments_optd.table_name(), "departments");

    // Test 4: Verify queries work with schema qualification
    let results = ctx
        .sql("SELECT * FROM public.users")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Verify exact user data
    let id_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_col = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(id_col.values(), &[1, 2]);
    assert_eq!(
        name_col.iter().collect::<Vec<_>>(),
        vec![Some("Alice"), Some("Bob")]
    );

    let results = ctx
        .sql("SELECT * FROM custom_schema.departments")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Verify exact department data
    let id_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let dept_col = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(id_col.values(), &[10, 20]);
    assert_eq!(
        dept_col.iter().collect::<Vec<_>>(),
        vec![Some("Engineering"), Some("Sales")]
    );
}

#[tokio::test]
async fn test_multiple_schemas_with_catalog_service() {
    let (_temp_dir, catalog) = create_test_catalog();
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let ctx = SessionContext::new();

    // Register tables in public schema
    let (_, batch1) = create_test_data(
        vec![("id", DataType::Int32), ("value", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ],
    );
    ctx.register_batch("table1", batch1).unwrap();

    // Create and register in custom schema
    let (_, batch2) = create_test_data(
        vec![("id", DataType::Int32), ("amount", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int32Array::from(vec![500, 600])),
        ],
    );

    let mem_table = MemTable::try_new(batch2.schema(), vec![vec![batch2]]).unwrap();
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema("analytics", Arc::new(MemorySchemaProvider::new()))
        .unwrap();

    ctx.catalog("datafusion")
        .unwrap()
        .schema("analytics")
        .unwrap()
        .register_table("table2".to_string(), Arc::new(mem_table))
        .unwrap();

    // Wrap with catalog service handle
    let catalog_list = ctx.state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(catalog_list, Some(handle.clone()));

    // Verify handle propagates to tables in both schemas
    let catalog_provider = optd_catalog_list.catalog("datafusion").unwrap();
    let optd_catalog = catalog_provider
        .as_any()
        .downcast_ref::<OptdCatalogProvider>()
        .expect("Should be OptdCatalogProvider");

    let table1 = catalog_provider
        .schema("public")
        .unwrap()
        .table("table1")
        .await
        .unwrap()
        .unwrap();
    let _table1_optd = table1.as_any().downcast_ref::<OptdTableProvider>().unwrap();

    let table2 = catalog_provider
        .schema("analytics")
        .unwrap()
        .table("table2")
        .await
        .unwrap()
        .unwrap();
    let _table2_optd = table2.as_any().downcast_ref::<OptdTableProvider>().unwrap();

    // Verify catalog has the handle (handle is at catalog level, not table level)
    let handle = optd_catalog
        .catalog_handle()
        .expect("catalog should have catalog handle");

    // Verify catalog service is accessible
    let snapshot = handle.current_snapshot().await.unwrap();
    assert_eq!(snapshot.0, 0, "Fresh catalog should start at snapshot 0");

    // Verify cross-schema query works
    let results = ctx
        .sql("SELECT t1.id, t1.value, t2.amount FROM public.table1 t1 CROSS JOIN analytics.table2 t2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        total_rows, 6,
        "3 rows from table1 * 2 rows from table2 = 6 rows"
    );

    // Verify exact cross join results
    let mut all_rows = Vec::new();
    for batch in &results {
        let t1_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let t1_value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let t2_amount = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            all_rows.push((t1_id.value(i), t1_value.value(i), t2_amount.value(i)));
        }
    }
    all_rows.sort();

    // Expected: each row from table1 (1,100), (2,200), (3,300) paired with each row from table2 (10,500), (20,600)
    assert_eq!(
        all_rows,
        vec![
            (1, 100, 500),
            (1, 100, 600),
            (2, 200, 500),
            (2, 200, 600),
            (3, 300, 500),
            (3, 300, 600),
        ]
    );
}
