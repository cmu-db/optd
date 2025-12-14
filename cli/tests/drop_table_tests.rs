use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionConfig};
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

/// Creates a test CLI context with persistent catalog
async fn create_cli_context_with_catalog(
    temp_dir: &TempDir,
) -> (OptdCliSessionContext, tokio::task::JoinHandle<()>) {
    let catalog_path = temp_dir.path().join("metadata.ducklake");
    let catalog = DuckLakeCatalog::try_new(None, Some(catalog_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    let service_handle = tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    // Wrap with OptD catalog
    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    (cli_ctx, service_handle)
}

#[tokio::test]
async fn test_drop_table() {
    let temp_dir = TempDir::new().unwrap();

    // Create test CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Session 1: Create table
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    let create_sql = format!(
        "CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify table exists with correct data
    let result = cli_ctx.inner().sql("SELECT * FROM test ORDER BY id").await.unwrap();
    let batches = datafusion::prelude::DataFrame::collect(result)
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 2, "Should have exactly 2 rows before drop");
    
    use datafusion::arrow::array::{Int64Array, StringArray};
    let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(id_col.value(0), 1, "First row should have id=1");
    assert_eq!(name_col.value(0), "Alice", "First row should be Alice");
    assert_eq!(id_col.value(1), 2, "Second row should have id=2");
    assert_eq!(name_col.value(1), "Bob", "Second row should be Bob");

    // Drop the table
    let drop_sql = "DROP TABLE test";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify table no longer exists
    let result = cli_ctx.inner().sql("SELECT * FROM test").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("test"));
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // DROP TABLE IF EXISTS on non-existent table should succeed
    let drop_sql = "DROP TABLE IF EXISTS nonexistent";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    let result = cli_ctx.execute_logical_plan(logical_plan).await;
    assert!(result.is_ok());

    // DROP TABLE (without IF EXISTS) on non-existent table should fail
    let drop_sql = "DROP TABLE nonexistent";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    let result = cli_ctx.execute_logical_plan(logical_plan).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("doesn't exist"));
}

#[tokio::test]
async fn test_drop_table_persists_across_sessions() {
    let temp_dir = TempDir::new().unwrap();

    // Create test CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Session 1: Create table
    {
        let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

        let create_sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
            csv_path.display()
        );
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&create_sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

        // Verify table exists with correct data
        let result = cli_ctx.inner().sql("SELECT * FROM test ORDER BY id").await.unwrap();
        let batches = datafusion::prelude::DataFrame::collect(result)
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 2, "Should have exactly 2 rows");
        
        use datafusion::arrow::array::{Int64Array, StringArray};
        let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(id_col.value(1), 2);
        assert_eq!(name_col.value(1), "Bob");

        // Drop the table
        let drop_sql = "DROP TABLE test";
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(drop_sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(logical_plan).await.unwrap();
    }

    // Session 2: Table should not be available (lazy loading should filter it out)
    {
        let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

        // Table should not exist in new session
        let result = cli_ctx.inner().sql("SELECT * FROM test").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test"));
    }
}

#[tokio::test]
async fn test_drop_table_multiple_tables() {
    let temp_dir = TempDir::new().unwrap();

    // Create test files
    let csv1_path = temp_dir.path().join("test1.csv");
    std::fs::write(&csv1_path, "id,name\n1,Alice\n").unwrap();

    let csv2_path = temp_dir.path().join("test2.csv");
    std::fs::write(&csv2_path, "id,name\n2,Bob\n").unwrap();

    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create two tables
    for (name, path) in [("test1", &csv1_path), ("test2", &csv2_path)] {
        let create_sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
            name,
            path.display()
        );
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&create_sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(logical_plan).await.unwrap();
    }

    // Verify both tables exist with correct data
    let result1 = cli_ctx.inner().sql("SELECT * FROM test1").await.unwrap();
    let batches1 = result1.collect().await.unwrap();
    assert_eq!(batches1[0].num_rows(), 1, "test1 should have 1 row");
    
    use datafusion::arrow::array::{Int64Array, StringArray};
    let id_col = batches1[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let name_col = batches1[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(name_col.value(0), "Alice");
    
    let result2 = cli_ctx.inner().sql("SELECT * FROM test2").await.unwrap();
    let batches2 = result2.collect().await.unwrap();
    assert_eq!(batches2[0].num_rows(), 1, "test2 should have 1 row");
    
    let id_col2 = batches2[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let name_col2 = batches2[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(id_col2.value(0), 2);
    assert_eq!(name_col2.value(0), "Bob");

    // Drop only test1
    let drop_sql = "DROP TABLE test1";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // test1 should not exist
    let result = cli_ctx.inner().sql("SELECT * FROM test1").await;
    assert!(result.is_err());

    // test2 should still exist with correct data
    let result = cli_ctx.inner().sql("SELECT * FROM test2").await;
    assert!(result.is_ok(), "test2 should still be accessible");
    let batches = result.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1, "test2 should still have 1 row");
    
    let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(id_col.value(0), 2, "test2 data should be unchanged");
    assert_eq!(name_col.value(0), "Bob", "test2 data should be unchanged");
}
