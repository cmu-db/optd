use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_statistics_available_after_create() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // Create a proper Parquet file using Arrow
    let parquet_path = temp_dir.path().join("stats_test.parquet");
    {
        use datafusion::arrow::array::{Int32Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::fs::File;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave", "Eve"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    let _service_handle = tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    // Create table (auto-stats should extract from Parquet)
    let create_sql = format!(
        "CREATE EXTERNAL TABLE stats_test STORED AS PARQUET LOCATION '{}'",
        parquet_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Retrieve statistics from catalog
    let snapshot = handle.current_snapshot().await.unwrap();
    let stats = handle
        .table_statistics("stats_test", snapshot)
        .await
        .unwrap();

    assert!(stats.is_some(), "Statistics should be available");
    let stats = stats.unwrap();
    
    assert_eq!(stats.row_count, 5, "Row count should be 5");
    assert_eq!(
        stats.column_statistics.len(),
        2,
        "Should have stats for 2 columns"
    );

    // Verify column names
    let col_names: Vec<_> = stats
        .column_statistics
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_statistics_versioning_across_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // Create two Parquet files with different row counts
    let parquet_path1 = temp_dir.path().join("v1.parquet");
    let parquet_path2 = temp_dir.path().join("v2.parquet");

    // Create first version with 3 rows
    {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::fs::File;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();

        let file = File::create(&parquet_path1).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // Create second version with 5 rows
    {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::fs::File;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();

        let file = File::create(&parquet_path2).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    let _service_handle = tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    // Create table v1
    let create_sql1 = format!(
        "CREATE EXTERNAL TABLE versioned STORED AS PARQUET LOCATION '{}'",
        parquet_path1.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql1)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    let snapshot1 = handle.current_snapshot().await.unwrap();
    let stats1 = handle
        .table_statistics("versioned", snapshot1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stats1.row_count, 3, "First version should have 3 rows");

    // Drop and recreate with v2 (simulates update)
    let drop_sql = "DROP TABLE versioned";
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(drop_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    let create_sql2 = format!(
        "CREATE EXTERNAL TABLE versioned STORED AS PARQUET LOCATION '{}'",
        parquet_path2.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql2)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    let snapshot2 = handle.current_snapshot().await.unwrap();
    let stats2 = handle
        .table_statistics("versioned", snapshot2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stats2.row_count, 5, "Second version should have 5 rows");

    // Verify we can still access old stats at old snapshot (time-travel)
    let stats1_again = handle
        .table_statistics("versioned", snapshot1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stats1_again.row_count, 3,
        "Old snapshot should still have old stats (time-travel support)"
    );

    handle.shutdown().await.unwrap();
}
