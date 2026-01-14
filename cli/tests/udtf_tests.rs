use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create a CLI context with catalog.
fn create_test_context() -> (TempDir, OptdCliSessionContext) {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // Create catalog and service
    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new().with_information_schema(true);
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    // Wrap with OptD catalog
    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    // Register UDTFs after catalog is set up
    cli_ctx.register_udtfs();

    (temp_dir, cli_ctx)
}

/// Helper to get current snapshot ID
async fn get_current_snapshot(cli_ctx: &OptdCliSessionContext) -> i64 {
    let df = cli_ctx
        .inner()
        .sql("SELECT MAX(snapshot_id) as snapshot_id FROM list_snapshots()")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_snapshots_udtf() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a test CSV file and table to generate a snapshot
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE test_table STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Test that list_snapshots() UDTF works
    let df = cli_ctx
        .inner()
        .sql("SELECT * FROM list_snapshots()")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    // Catalog starts with snapshot 0, CREATE TABLE creates snapshot 1
    // So we should have 2 snapshots: 0 and 1
    assert_eq!(batches[0].num_rows(), 2, "Should have snapshots 0 and 1");
    assert_eq!(batches[0].num_columns(), 4);

    // Verify column names
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "snapshot_id");
    assert_eq!(schema.field(1).name(), "schema_version");
    assert_eq!(schema.field(2).name(), "next_catalog_id");
    assert_eq!(schema.field(3).name(), "next_file_id");

    // Verify snapshots are 0 and 1
    let snapshot_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        snapshot_ids.value(0),
        0,
        "First snapshot should be 0 (initial)"
    );
    assert_eq!(
        snapshot_ids.value(1),
        1,
        "Second snapshot should be 1 (after CREATE TABLE)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_tables_at_snapshot_udtf() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a test CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Create external table
    let create_sql = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Test that list_tables_at_snapshot(1) UDTF works
    let df = cli_ctx
        .inner()
        .sql("SELECT * FROM list_tables_at_snapshot(1)")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1, "Should have 1 table");
    assert_eq!(batches[0].num_columns(), 4);

    // Verify column names
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "table_name");
    assert_eq!(schema.field(1).name(), "location");
    assert_eq!(schema.field(2).name(), "file_format");
    assert_eq!(schema.field(3).name(), "compression");

    // Verify table name
    let table_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_names.value(0), "users");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_snapshots_udtf_with_where_clause() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a table to generate snapshot 1
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE test_table STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Test filtering snapshots with WHERE clause
    let df = cli_ctx
        .inner()
        .sql("SELECT snapshot_id, schema_version FROM list_snapshots() WHERE snapshot_id = 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1, "Should find snapshot 1");
    assert_eq!(batches[0].num_columns(), 2);

    let snapshot_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        snapshot_ids.value(0),
        1,
        "Filtered snapshot should have ID 1"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_udtf_works_with_join() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a test CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Create external table
    let create_sql = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Test UDTF in a JOIN with literal value
    // Note: DataFusion UDTFs can only accept literal values, not column references
    let df = cli_ctx
        .inner()
        .sql(
            "SELECT s.snapshot_id, t.table_name 
             FROM list_snapshots() s 
             CROSS JOIN list_tables_at_snapshot(1) t 
             WHERE s.snapshot_id = 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].num_rows(),
        1,
        "Should have 1 row from CROSS JOIN"
    );

    // Verify we got both snapshot and table data
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "snapshot_id");
    assert_eq!(schema.field(1).name(), "table_name");

    let table_names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_names.value(0), "users");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_multiple_tables_at_snapshot() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create multiple test files
    let csv_path = temp_dir.path().join("users.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    let json_path = temp_dir.path().join("orders.json");
    std::fs::write(&json_path, r#"{"id": 1, "total": 100.0}"#).unwrap();

    // Create first table
    let create_sql1 = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan1 = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql1)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan1).await.unwrap();

    // Create second table
    let create_sql2 = format!(
        "CREATE EXTERNAL TABLE orders STORED AS JSON LOCATION '{}'",
        json_path.display()
    );
    let logical_plan2 = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql2)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan2).await.unwrap();

    // Get current snapshot and list all tables
    let df = cli_ctx
        .inner()
        .sql("SELECT MAX(snapshot_id) as sid FROM list_snapshots()")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let snapshot_id = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    // List tables at current snapshot
    let df = cli_ctx
        .inner()
        .sql(&format!(
            "SELECT * FROM list_tables_at_snapshot({}) ORDER BY table_name",
            snapshot_id
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 tables");
    assert_eq!(batches[0].num_columns(), 4);

    // Verify table names
    let table_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_names.value(0), "orders");
    assert_eq!(table_names.value(1), "users");

    // Verify file formats
    let formats = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(formats.value(0), "JSON");
    assert_eq!(formats.value(1), "CSV");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_tables_empty_snapshot() {
    let (_temp_dir, cli_ctx) = create_test_context();

    // Get current snapshot (should be 0 - initial snapshot with no tables)
    let snapshot_id = get_current_snapshot(&cli_ctx).await;

    let df = cli_ctx
        .inner()
        .sql(&format!(
            "SELECT * FROM list_tables_at_snapshot({})",
            snapshot_id
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].num_rows(),
        0,
        "Should have no tables at initial snapshot"
    );
    assert_eq!(batches[0].num_columns(), 4);

    // Verify column names
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "table_name");
    assert_eq!(schema.field(1).name(), "location");
    assert_eq!(schema.field(2).name(), "file_format");
    assert_eq!(schema.field(3).name(), "compression");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_tables_single_table_metadata() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a test CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Create external table
    let create_sql = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Get current snapshot and query tables
    let snapshot_id = get_current_snapshot(&cli_ctx).await;
    let df = cli_ctx
        .inner()
        .sql(&format!(
            "SELECT * FROM list_tables_at_snapshot({})",
            snapshot_id
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1, "Should have 1 table");

    // Verify table name
    let table_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_names.value(0), "users");

    // Verify exact location path
    let locations = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let expected_location = csv_path.to_str().unwrap();
    assert_eq!(
        locations.value(0),
        expected_location,
        "Location should match exact file path"
    );

    // Verify exact format (case-sensitive)
    let formats = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(formats.value(0), "CSV", "Format should be exactly 'CSV'");

    // Verify compression is None for uncompressed file
    let compressions = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        compressions.is_null(0),
        "Compression should be NULL for uncompressed file"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_tables_multiple_formats() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create test files with different formats
    let csv_path = temp_dir.path().join("users.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    let json_path = temp_dir.path().join("orders.json");
    std::fs::write(&json_path, r#"{"id": 1, "total": 100.0}"#).unwrap();

    // Create multiple external tables
    let create_sql1 = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan1 = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql1)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan1).await.unwrap();

    let create_sql2 = format!(
        "CREATE EXTERNAL TABLE orders STORED AS JSON LOCATION '{}'",
        json_path.display()
    );
    let logical_plan2 = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql2)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan2).await.unwrap();

    // Get current snapshot and query tables
    let snapshot_id = get_current_snapshot(&cli_ctx).await;
    let df = cli_ctx
        .inner()
        .sql(&format!(
            "SELECT * FROM list_tables_at_snapshot({}) ORDER BY table_name",
            snapshot_id
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2, "Should have exactly 2 tables");

    // Verify table names
    let table_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let names: Vec<&str> = (0..table_names.len())
        .map(|i| table_names.value(i))
        .collect();

    assert!(names.contains(&"users"), "Should contain users table");
    assert!(names.contains(&"orders"), "Should contain orders table");

    // Verify formats match table types
    let formats = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Find users table and verify CSV format
    let users_idx = names.iter().position(|&n| n == "users").unwrap();
    assert_eq!(
        formats.value(users_idx),
        "CSV",
        "users table should be CSV format"
    );

    // Find orders table and verify JSON format
    let orders_idx = names.iter().position(|&n| n == "orders").unwrap();
    assert_eq!(
        formats.value(orders_idx),
        "JSON",
        "orders table should be JSON format"
    );

    // Verify locations are correct
    let locations = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(
        locations.value(users_idx),
        csv_path.to_str().unwrap(),
        "users location should match CSV path"
    );
    assert_eq!(
        locations.value(orders_idx),
        json_path.to_str().unwrap(),
        "orders location should match JSON path"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_tables_after_drop() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create test files
    let csv1_path = temp_dir.path().join("table1.csv");
    std::fs::write(&csv1_path, "id\n1\n").unwrap();

    let csv2_path = temp_dir.path().join("table2.csv");
    std::fs::write(&csv2_path, "id\n2\n").unwrap();

    // Create two tables
    let create_sql1 = format!(
        "CREATE EXTERNAL TABLE table1 STORED AS CSV LOCATION '{}'",
        csv1_path.display()
    );
    let logical_plan1 = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql1)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan1).await.unwrap();

    let create_sql2 = format!(
        "CREATE EXTERNAL TABLE table2 STORED AS CSV LOCATION '{}'",
        csv2_path.display()
    );
    let logical_plan2 = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql2)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan2).await.unwrap();

    // Drop one table
    let drop_sql = "DROP TABLE table1";
    let drop_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(drop_plan).await.unwrap();

    // Get current snapshot and query tables
    let snapshot_id = get_current_snapshot(&cli_ctx).await;
    let df = cli_ctx
        .inner()
        .sql(&format!(
            "SELECT * FROM list_tables_at_snapshot({})",
            snapshot_id
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].num_rows(),
        1,
        "Should have exactly 1 table remaining"
    );

    // Verify only table2 remains with correct metadata
    let table_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        table_names.value(0),
        "table2",
        "Remaining table should be table2"
    );

    // Verify table2 location and format are correct
    let locations = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        locations.value(0),
        csv2_path.to_str().unwrap(),
        "table2 location should match its CSV path"
    );

    let formats = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(formats.value(0), "CSV", "table2 format should be CSV");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_tables_with_semicolon() {
    let (_temp_dir, cli_ctx) = create_test_context();

    // Get current snapshot and test that semicolon is handled correctly
    let snapshot_id = get_current_snapshot(&cli_ctx).await;
    let df = cli_ctx
        .inner()
        .sql(&format!(
            "SELECT * FROM list_tables_at_snapshot({});",
            snapshot_id
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].num_rows(),
        0,
        "Should have no tables at initial snapshot"
    );
}
