// Additional comprehensive tests for CREATE/DROP TABLE edge cases

use datafusion::{
    arrow::array::{Int32Array, RecordBatch},
    arrow::datatypes::{DataType, Field, Schema},
    execution::runtime_env::RuntimeEnvBuilder,
    prelude::SessionConfig,
};
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

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    (cli_ctx, service_handle)
}

#[tokio::test]
async fn test_create_table_with_compression() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create regular CSV file (uncompressed)
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Create table WITHOUT compression first - this will work
    let create_sql = format!(
        "CREATE EXTERNAL TABLE test_no_compression STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify it works with correct data
    let result = cli_ctx
        .inner()
        .sql("SELECT * FROM test_no_compression ORDER BY id")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2, "Should have exactly 2 rows");
    
    use datafusion::arrow::array::{Int64Array, StringArray};
    let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(id_col.value(1), 2);
    assert_eq!(name_col.value(1), "Bob");

    // New session - verify table persists with its options and data
    let (cli_ctx2, _service_handle2) = create_cli_context_with_catalog(&temp_dir).await;
    let result = cli_ctx2
        .inner()
        .sql("SELECT * FROM test_no_compression ORDER BY id")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(
        batches[0].num_rows(),
        2,
        "Table with options should persist"
    );
    
    let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(id_col.value(0), 1, "Persisted data should match original");
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(id_col.value(1), 2);
    assert_eq!(name_col.value(1), "Bob");
}

#[tokio::test]
async fn test_create_drop_recreate_table() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create table
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

    // Verify it works
    let result = cli_ctx
        .inner()
        .sql("SELECT COUNT(*) FROM test")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    // Drop it
    let drop_sql = "DROP TABLE test";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Recreate with same name
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Should work again
    let result = cli_ctx
        .inner()
        .sql("SELECT COUNT(*) FROM test")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_multiple_file_formats() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // CSV
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n")?;

    // JSON
    let json_path = temp_dir.path().join("test.json");
    std::fs::write(&json_path, r#"{"id": 2, "name": "Bob"}"#)?;

    // Parquet
    let parquet_path = temp_dir.path().join("test.parquet");
    {
        use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
        use std::fs::File;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["Carol"])),
            ],
        )?;

        let file = File::create(&parquet_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    // Create all three tables
    let formats = vec![
        (
            "csv_table",
            &csv_path,
            "CSV",
            "OPTIONS ('format.has_header' 'true')",
        ),
        ("json_table", &json_path, "JSON", ""),
        ("parquet_table", &parquet_path, "PARQUET", ""),
    ];

    for (name, path, format, options) in formats {
        let create_sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS {} LOCATION '{}' {}",
            name,
            format,
            path.display(),
            options
        );
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&create_sql)
            .await?;
        cli_ctx.execute_logical_plan(logical_plan).await?;
    }

    // Verify all tables exist
    cli_ctx.inner().sql("SELECT * FROM csv_table").await?;
    cli_ctx.inner().sql("SELECT * FROM json_table").await?;
    cli_ctx.inner().sql("SELECT * FROM parquet_table").await?;

    // New session - all should persist
    let (cli_ctx2, _service_handle2) = create_cli_context_with_catalog(&temp_dir).await;
    cli_ctx2.inner().sql("SELECT * FROM csv_table").await?;
    cli_ctx2.inner().sql("SELECT * FROM json_table").await?;
    cli_ctx2.inner().sql("SELECT * FROM parquet_table").await?;

    Ok(())
}

#[tokio::test]
async fn test_custom_table_options() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id;name\n1;Alice\n2;Bob\n").unwrap();

    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create table with custom delimiter
    let create_sql = format!(
        "CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true', 'format.delimiter' ';')",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify it works with custom delimiter
    let result = cli_ctx.inner().sql("SELECT * FROM test").await.unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);

    // New session - custom options should persist
    let (cli_ctx2, _service_handle2) = create_cli_context_with_catalog(&temp_dir).await;
    let result = cli_ctx2.inner().sql("SELECT * FROM test").await.unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn test_drop_table_after_queries() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n3,Carol\n").unwrap();

    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create table
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

    // Run multiple queries
    for _ in 0..3 {
        let result = cli_ctx
            .inner()
            .sql("SELECT COUNT(*) FROM test")
            .await
            .unwrap();
        result.collect().await.unwrap();
    }

    // Now drop it
    let drop_sql = "DROP TABLE test";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Should not exist
    let result = cli_ctx.inner().sql("SELECT * FROM test").await;
    assert!(result.is_err());

    // New session - should still not exist
    let (cli_ctx2, _service_handle2) = create_cli_context_with_catalog(&temp_dir).await;
    let result = cli_ctx2.inner().sql("SELECT * FROM test").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_if_exists_idempotent() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Multiple DROP IF EXISTS on non-existent table should all succeed
    for _ in 0..3 {
        let drop_sql = "DROP TABLE IF EXISTS nonexistent";
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(drop_sql)
            .await
            .unwrap();
        let result = cli_ctx.execute_logical_plan(logical_plan).await;
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_table_name_variations() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id\n1\n").unwrap();

    // Test simple name
    let create_sql = format!(
        "CREATE EXTERNAL TABLE my_table STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Verify it exists
    cli_ctx.inner().sql("SELECT * FROM my_table").await.unwrap();

    // Drop it
    let drop_sql = "DROP TABLE my_table";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Test name with numbers
    let create_sql = format!(
        "CREATE EXTERNAL TABLE table123 STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    cli_ctx.inner().sql("SELECT * FROM table123").await.unwrap();
}

#[tokio::test]
async fn test_empty_table_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("empty.csv");
    std::fs::write(&csv_path, "id,name,age\n").unwrap(); // Header only

    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    let create_sql = format!(
        "CREATE EXTERNAL TABLE empty STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Query should return 0 rows but preserve schema
    let result = cli_ctx.inner().sql("SELECT * FROM empty").await.unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches.len(), 0, "Empty table should return 0 batches");
    
    // Verify schema is still accessible via LIMIT 0
    let result_schema = cli_ctx.inner().sql("SELECT * FROM empty LIMIT 0").await.unwrap();
    let schema_batches = result_schema.collect().await.unwrap();
    if !schema_batches.is_empty() {
        let schema = schema_batches[0].schema();
        assert_eq!(schema.fields().len(), 3, "Should have 3 columns");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "age");
    }

    // New session - should still work with same schema
    let (cli_ctx2, _service_handle2) = create_cli_context_with_catalog(&temp_dir).await;
    let result = cli_ctx2.inner().sql("SELECT * FROM empty").await.unwrap();
    let batches = result.collect().await.unwrap();
    assert_eq!(batches.len(), 0, "Empty table should still return 0 batches after restart");
}
