use datafusion::arrow::array::{Array, StringArray};
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

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    // Wrap with OptD catalog
    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    (temp_dir, cli_ctx)
}

#[tokio::test]
async fn test_show_tables_empty() {
    let (_temp_dir, cli_ctx) = create_test_context();

    let df = cli_ctx.sql_with_optd("SHOW TABLES").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(batches[0].num_columns(), 4);

    // Verify column names
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "table_name");
    assert_eq!(schema.field(1).name(), "location");
    assert_eq!(schema.field(2).name(), "file_format");
    assert_eq!(schema.field(3).name(), "compression");
}

#[tokio::test]
async fn test_show_tables_single() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a test CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Create external table using execute_logical_plan to trigger persistence
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

    // Execute SHOW TABLES
    let df = cli_ctx.sql_with_optd("SHOW TABLES").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

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
    assert!(compressions.is_null(0), "Compression should be NULL for uncompressed file");
}

#[tokio::test]
async fn test_show_tables_multiple() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create test files
    let csv_path = temp_dir.path().join("users.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    let json_path = temp_dir.path().join("orders.json");
    std::fs::write(&json_path, r#"{"id": 1, "total": 100.0}"#).unwrap();

    // Create multiple external tables using execute_logical_plan
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

    // Execute SHOW TABLES
    let df = cli_ctx.sql_with_optd("SHOW TABLES").await.unwrap();
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
    assert_eq!(formats.value(users_idx), "CSV", "users table should be CSV format");
    
    // Find orders table and verify JSON format
    let orders_idx = names.iter().position(|&n| n == "orders").unwrap();
    assert_eq!(formats.value(orders_idx), "JSON", "orders table should be JSON format");
    
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

#[tokio::test]
async fn test_show_tables_after_drop() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create test files
    let csv1_path = temp_dir.path().join("table1.csv");
    std::fs::write(&csv1_path, "id\n1\n").unwrap();

    let csv2_path = temp_dir.path().join("table2.csv");
    std::fs::write(&csv2_path, "id\n2\n").unwrap();

    // Create two tables using execute_logical_plan
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

    // Drop one table using execute_logical_plan
    let drop_sql = "DROP TABLE table1";
    let drop_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(drop_plan).await.unwrap();

    // Execute SHOW TABLES
    let df = cli_ctx.sql_with_optd("SHOW TABLES").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1, "Should have exactly 1 table remaining");

    // Verify only table2 remains with correct metadata
    let table_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_names.value(0), "table2", "Remaining table should be table2");
    
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

#[tokio::test]
async fn test_show_tables_with_semicolon() {
    let (_temp_dir, cli_ctx) = create_test_context();

    let df = cli_ctx.sql_with_optd("SHOW TABLES;").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
}
