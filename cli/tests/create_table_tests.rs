// Integration test for CREATE EXTERNAL TABLE persistence

use datafusion::{
    arrow::array::{Int32Array, RecordBatch},
    arrow::datatypes::{DataType, Field, Schema},
    execution::runtime_env::RuntimeEnvBuilder,
    prelude::SessionConfig,
};
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_create_external_table_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let csv_path = temp_dir.path().join("test.csv");

    // Create test CSV file
    std::fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Carol,35\n")?;

    // Create external table
    {
        let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap()))?;
        let (service, handle) = CatalogService::new(catalog);
        tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        // Wrap with OptD catalog
        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // CREATE EXTERNAL TABLE via CLI context
        let sql = format!(
            "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
            csv_path.display()
        );

        // Use CliSessionContext to execute
        use datafusion_cli::cli_context::CliSessionContext;
        let logical_plan = cli_ctx.inner().state().create_logical_plan(&sql).await?;
        cli_ctx.execute_logical_plan(logical_plan).await?;

        let result = cli_ctx
            .inner()
            .sql("SELECT * FROM users ORDER BY id")
            .await?;
        let batches = result.collect().await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    // Verify table persists after restart
    {
        let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap()))?;
        let (service, handle) = CatalogService::new(catalog);
        tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        // Wrap with OptD catalog
        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // Table should be accessible via lazy loading
        let result = cli_ctx
            .inner()
            .sql("SELECT * FROM users ORDER BY id")
            .await?;
        let batches = result.collect().await?;

        assert_eq!(batches.len(), 1, "Should have one batch");
        assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

        let schema = batches[0].schema();
        println!("Schema: {:?}", schema);
        assert_eq!(
            schema.fields().len(),
            3,
            "Should have exactly 3 columns (id, name, age)"
        );
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "age");

        // Validate actual data values
        use datafusion::arrow::array::{Int64Array, StringArray};
        let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let age_col = batches[0].column(2).as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(id_col.value(0), 1, "First row ID should be 1");
        assert_eq!(name_col.value(0), "Alice", "First row name should be Alice");
        assert_eq!(age_col.value(0), 30, "First row age should be 30");

        assert_eq!(id_col.value(1), 2, "Second row ID should be 2");
        assert_eq!(name_col.value(1), "Bob", "Second row name should be Bob");
        assert_eq!(age_col.value(1), 25, "Second row age should be 25");

        assert_eq!(id_col.value(2), 3, "Third row ID should be 3");
        assert_eq!(name_col.value(2), "Carol", "Third row name should be Carol");
        assert_eq!(age_col.value(2), 35, "Third row age should be 35");
    }

    Ok(())
}

#[tokio::test]
async fn test_multiple_external_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let csv_path = temp_dir.path().join("users.csv");
    let parquet_path = temp_dir.path().join("orders.parquet");

    // Create test CSV file
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n")?;

    // Create test Parquet file
    {
        use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
        use std::fs::File;

        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![101, 102])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )?;

        let file = File::create(&parquet_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    // Create both tables
    {
        let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap()))?;
        let (service, handle) = CatalogService::new(catalog);
        tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        use datafusion_cli::cli_context::CliSessionContext;

        // Create CSV table
        let sql = format!(
            "CREATE EXTERNAL TABLE users STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
            csv_path.display()
        );
        let logical_plan = cli_ctx.inner().state().create_logical_plan(&sql).await?;
        cli_ctx.execute_logical_plan(logical_plan).await?;

        // Create Parquet table
        let sql = format!(
            "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION '{}'",
            parquet_path.display()
        );
        let logical_plan = cli_ctx.inner().state().create_logical_plan(&sql).await?;
        cli_ctx.execute_logical_plan(logical_plan).await?;

        // Query both tables and validate data
        let result = cli_ctx.inner().sql("SELECT * FROM users ORDER BY id").await?;
        let batches = result.collect().await?;
        assert_eq!(batches[0].num_rows(), 2, "Users table should have 2 rows");
        
        use datafusion::arrow::array::{Int64Array, StringArray};
        let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(id_col.value(1), 2);
        assert_eq!(name_col.value(1), "Bob");

        let result = cli_ctx.inner().sql("SELECT * FROM orders ORDER BY order_id").await?;
        let batches = result.collect().await?;
        assert_eq!(batches[0].num_rows(), 2, "Orders table should have 2 rows");
        
        use datafusion::arrow::array::Int32Array;
        let order_id_col = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let user_id_col = batches[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(order_id_col.value(0), 101);
        assert_eq!(user_id_col.value(0), 1);
        assert_eq!(order_id_col.value(1), 102);
        assert_eq!(user_id_col.value(1), 2);
    }

    // Verify both persist
    {
        let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap()))?;
        let (service, handle) = CatalogService::new(catalog);
        tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // Both tables should work after restart with exact data
        let result = cli_ctx.inner().sql("SELECT * FROM users ORDER BY id").await?;
        let batches = result.collect().await?;
        assert_eq!(batches[0].num_rows(), 2, "CSV table should persist");
        
        use datafusion::arrow::array::{Int64Array, StringArray};
        let id_col = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_col.value(0), 1, "Persisted CSV data should match");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(id_col.value(1), 2);
        assert_eq!(name_col.value(1), "Bob");

        let result = cli_ctx.inner().sql("SELECT * FROM orders ORDER BY order_id").await?;
        let batches = result.collect().await?;
        assert_eq!(batches[0].num_rows(), 2, "Parquet table should persist");
        
        use datafusion::arrow::array::Int32Array;
        let order_id_col = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let user_id_col = batches[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(order_id_col.value(0), 101, "Persisted Parquet data should match");
        assert_eq!(user_id_col.value(0), 1);
        assert_eq!(order_id_col.value(1), 102);
        assert_eq!(user_id_col.value(1), 2);
    }

    Ok(())
}

#[tokio::test]
async fn test_create_external_table_if_not_exists() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let csv_path = temp_dir.path().join("test.csv");

    std::fs::write(&csv_path, "id\n1\n2\n")?;

    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap()))?;
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc()?;
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    use datafusion_cli::cli_context::CliSessionContext;

    // Create table
    let sql = format!(
        "CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx.inner().state().create_logical_plan(&sql).await?;
    cli_ctx.execute_logical_plan(logical_plan).await?;

    // Try to create again
    let logical_plan = cli_ctx.inner().state().create_logical_plan(&sql).await?;
    let result = cli_ctx.execute_logical_plan(logical_plan).await;
    assert!(result.is_err(), "Should fail on duplicate table creation");

    // Try with IF NOT EXISTS
    let sql_if_not_exists = format!(
        "CREATE EXTERNAL TABLE IF NOT EXISTS test STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&sql_if_not_exists)
        .await?;
    cli_ctx.execute_logical_plan(logical_plan).await?;

    Ok(())
}
