use datafusion::arrow::array::{Array, Int64Array};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{Catalog, CatalogService, DuckLakeCatalog, RegisterTableRequest};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::collections::HashMap;
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

    // Register UDTFs
    cli_ctx.register_udtfs();

    (temp_dir, cli_ctx)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_create_and_query_table() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create a CSV file
    let csv_path = temp_dir.path().join("events.csv");
    std::fs::write(&csv_path, "id,event_type\n1,login\n2,logout\n").unwrap();

    // Create table in default (public) schema
    let create_sql = format!(
        "CREATE EXTERNAL TABLE events STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Query the table
    let df = cli_ctx.inner().sql("SELECT * FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    // Verify data
    let id_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_array.value(0), 1);
    assert_eq!(id_array.value(1), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_multiple_tables() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create CSV files
    let csv_path1 = temp_dir.path().join("users.csv");
    std::fs::write(&csv_path1, "id,name\n1,Alice\n").unwrap();

    let csv_path2 = temp_dir.path().join("orders.csv");
    std::fs::write(&csv_path2, "id,user_id\n1,1\n").unwrap();

    // Create first table
    let create_sql1 = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        csv_path1.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql1)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Create second table
    let create_sql2 = format!(
        "CREATE EXTERNAL TABLE orders STORED AS CSV LOCATION '{}'",
        csv_path2.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql2)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify both tables exist by querying them directly
    let df1 = cli_ctx.inner().sql("SELECT * FROM users").await.unwrap();
    let batches1 = df1.collect().await.unwrap();
    assert_eq!(batches1[0].num_rows(), 1, "users table should have 1 row");

    let df2 = cli_ctx.inner().sql("SELECT * FROM orders").await.unwrap();
    let batches2 = df2.collect().await.unwrap();
    assert_eq!(batches2[0].num_rows(), 1, "orders table should have 1 row");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_join_tables() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create users table
    let users_csv = temp_dir.path().join("users.csv");
    std::fs::write(&users_csv, "user_id,name\n1,Alice\n2,Bob\n").unwrap();

    let create_users = format!(
        "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}'",
        users_csv.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_users)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Create events table
    let events_csv = temp_dir.path().join("events.csv");
    std::fs::write(&events_csv, "user_id,event\n1,login\n2,purchase\n").unwrap();

    let create_events = format!(
        "CREATE EXTERNAL TABLE events STORED AS CSV LOCATION '{}'",
        events_csv.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_events)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // JOIN tables
    let df = cli_ctx
        .inner()
        .sql("SELECT users.name, events.event FROM users JOIN events ON users.user_id = events.user_id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    // Count total rows across all batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should have 2 joined rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_drop_table() {
    let (temp_dir, cli_ctx) = create_test_context();

    // Create table
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id\n1\n").unwrap();

    let create_table = format!(
        "CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_table)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify table exists
    let df = cli_ctx.inner().sql("SELECT * FROM test").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    // Drop the table
    let drop_table = "DROP TABLE test";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_table)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify table no longer exists
    let result = cli_ctx.inner().sql("SELECT * FROM test").await;
    assert!(result.is_err(), "Table should not exist after DROP");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_create_schema_via_catalog() {
    let (temp_dir, _cli_ctx) = create_test_context();

    // Get catalog handle to create schema (CLI doesn't expose CREATE SCHEMA directly)
    let catalog = DuckLakeCatalog::try_new(
        None,
        Some(temp_dir.path().join("metadata.ducklake").to_str().unwrap()),
    )
    .unwrap();

    // Create a schema through catalog
    let mut catalog_mut = catalog;
    catalog_mut.create_schema("analytics").unwrap();

    // Verify schema exists by listing
    let schemas = catalog_mut.list_schemas().unwrap();
    assert!(
        schemas.contains(&"analytics".to_string()),
        "analytics schema should exist"
    );
    assert!(
        schemas.contains(&"main".to_string()),
        "main schema should exist"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_schema_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // Setup catalog with two schemas
    let mut catalog =
        DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    catalog.create_schema("production").unwrap();

    // Register table with same name in different schemas
    let csv_path1 = temp_dir.path().join("main_data.csv");
    std::fs::write(&csv_path1, "id,value\n1,100\n").unwrap();

    let csv_path2 = temp_dir.path().join("prod_data.csv");
    std::fs::write(&csv_path2, "id,value\n1,200\n").unwrap();

    // Register "data" in main schema
    let request1 = RegisterTableRequest {
        table_name: "data".to_string(),
        schema_name: None,
        location: csv_path1.to_str().unwrap().to_string(),
        file_format: "csv".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request1).unwrap();

    // Register "data" in production schema
    let request2 = RegisterTableRequest {
        table_name: "data".to_string(),
        schema_name: Some("production".to_string()),
        location: csv_path2.to_str().unwrap().to_string(),
        file_format: "csv".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request2).unwrap();

    // Verify isolation: main.data and production.data are different
    let main_table = catalog.get_external_table(None, "data").unwrap().unwrap();
    let prod_table = catalog
        .get_external_table(Some("production"), "data")
        .unwrap()
        .unwrap();

    assert_eq!(main_table.table_name, "data");
    assert_eq!(prod_table.table_name, "data");
    assert_ne!(
        main_table.location, prod_table.location,
        "Tables in different schemas should have different locations"
    );
    assert!(main_table.location.contains("main_data.csv"));
    assert!(prod_table.location.contains("prod_data.csv"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_list_tables_per_schema() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let mut catalog =
        DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    catalog.create_schema("staging").unwrap();

    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id\n1\n").unwrap();

    // Register 2 tables in main, 1 in staging
    for (schema, table_name) in [
        (None, "table1"),
        (None, "table2"),
        (Some("staging"), "table3"),
    ] {
        let request = RegisterTableRequest {
            table_name: table_name.to_string(),
            schema_name: schema.map(|s| s.to_string()),
            location: csv_path.to_str().unwrap().to_string(),
            file_format: "csv".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        catalog.register_external_table(request).unwrap();
    }

    // List tables in main schema
    let main_tables = catalog.list_external_tables(None).unwrap();
    assert_eq!(
        main_tables.len(),
        2,
        "main schema should have exactly 2 tables"
    );
    assert!(main_tables.iter().any(|t| t.table_name == "table1"));
    assert!(main_tables.iter().any(|t| t.table_name == "table2"));

    // List tables in staging schema
    let staging_tables = catalog.list_external_tables(Some("staging")).unwrap();
    assert_eq!(
        staging_tables.len(),
        1,
        "staging schema should have exactly 1 table"
    );
    assert_eq!(staging_tables[0].table_name, "table3");
}
