//! Tests for automatic statistics computation.
//!
//! Verifies that statistics are automatically computed and stored when creating
//! external tables with supported file formats.

use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionConfig};
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogService, CatalogServiceHandle, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

/// Creates a test CLI context with persistent catalog
async fn create_cli_context_with_catalog(
    temp_dir: &TempDir,
) -> (
    OptdCliSessionContext,
    CatalogServiceHandle,
    tokio::task::JoinHandle<()>,
) {
    let catalog_path = temp_dir.path().join("metadata.ducklake");

    // Create catalog for service
    let catalog = DuckLakeCatalog::try_new(None, Some(catalog_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    let service_handle = tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list =
        OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    (cli_ctx, handle, service_handle)
}

/// Helper to create a test Parquet file
async fn create_test_parquet_file(path: &str, num_rows: usize) {
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create sample data
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut ages = Vec::new();

    for i in 0..num_rows {
        ids.push(i as i32);
        names.push(format!("User{}", i));
        ages.push(20 + (i % 50) as i32);
    }

    let batch = datafusion::arrow::record_batch::RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(datafusion::arrow::array::Int32Array::from(ids))
                as Arc<dyn datafusion::arrow::array::Array>,
        ),
        (
            "name",
            Arc::new(datafusion::arrow::array::StringArray::from(names))
                as Arc<dyn datafusion::arrow::array::Array>,
        ),
        (
            "age",
            Arc::new(datafusion::arrow::array::Int32Array::from(ages))
                as Arc<dyn datafusion::arrow::array::Array>,
        ),
    ])
    .unwrap();

    let df = ctx.read_batch(batch).unwrap();
    df.write_parquet(
        path,
        datafusion::dataframe::DataFrameWriteOptions::new(),
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_auto_stats_parquet_enabled_by_default() {
    // Auto-stats should be enabled by default for Parquet files
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("test_users.parquet");
    create_test_parquet_file(parquet_path.to_str().unwrap(), 100).await;

    let (cli_ctx, catalog_handle, _service_handle) =
        create_cli_context_with_catalog(&temp_dir).await;

    // Create external table
    let sql = format!(
        "CREATE EXTERNAL TABLE test_users STORED AS PARQUET LOCATION '{}'",
        parquet_path.to_str().unwrap()
    );

    let plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(plan).await.unwrap();

    // Query through the service handle using get_table_statistics_manual (for external tables)
    let stats = catalog_handle
        .get_table_statistics_manual(None, "test_users")
        .await
        .unwrap();

    assert!(
        stats.is_some(),
        "Statistics should be auto-computed for Parquet"
    );
    let stats = stats.unwrap();
    assert_eq!(
        stats.row_count, 100,
        "Row count should match Parquet metadata"
    );
}

#[tokio::test]
async fn test_auto_stats_parquet_row_count_accuracy() {
    // Verify that row count extracted from Parquet metadata is accurate for different sizes
    let test_cases = vec![1, 50, 1000];

    for num_rows in test_cases {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join(format!("test_{}.parquet", num_rows));
        create_test_parquet_file(parquet_path.to_str().unwrap(), num_rows).await;

        let (cli_ctx, catalog_handle, _service_handle) =
            create_cli_context_with_catalog(&temp_dir).await;

        let sql = format!(
            "CREATE EXTERNAL TABLE test_{} STORED AS PARQUET LOCATION '{}'",
            num_rows,
            parquet_path.to_str().unwrap()
        );

        let plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(plan).await.unwrap();

        let stats = catalog_handle
            .get_table_statistics_manual(None, &format!("test_{}", num_rows))
            .await
            .unwrap();

        assert!(
            stats.is_some(),
            "Statistics should exist for {} rows",
            num_rows
        );
        assert_eq!(
            stats.unwrap().row_count,
            num_rows,
            "Row count should be {} for test table",
            num_rows
        );
    }
}

#[tokio::test]
async fn test_auto_stats_disabled_for_csv_by_default() {
    // Auto-stats should be disabled by default for CSV files (due to cost)
    let temp_dir = TempDir::new().unwrap();
    let csv_path = temp_dir.path().join("test.csv");

    // Create a simple CSV file
    std::fs::write(&csv_path, "id,name,age\n1,Alice,25\n2,Bob,30\n").unwrap();

    let (cli_ctx, catalog_handle, _service_handle) =
        create_cli_context_with_catalog(&temp_dir).await;

    let sql = format!(
        "CREATE EXTERNAL TABLE test_csv STORED AS CSV LOCATION '{}' OPTIONS('format.has_header' 'true')",
        csv_path.to_str().unwrap()
    );

    let plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(plan).await.unwrap();

    // Check that statistics were NOT auto-computed (CSV disabled by default)
    let stats = catalog_handle
        .get_table_statistics_manual(None, "test_csv")
        .await
        .unwrap();

    assert!(
        stats.is_none(),
        "Statistics should NOT be auto-computed for CSV by default"
    );
}

#[tokio::test]
async fn test_auto_stats_disabled_for_json_by_default() {
    // Auto-stats should be disabled by default for JSON files (due to cost)
    let temp_dir = TempDir::new().unwrap();
    let json_path = temp_dir.path().join("test.json");

    // Create a simple JSON file
    std::fs::write(
        &json_path,
        r#"{"id":1,"name":"Alice","age":25}
{"id":2,"name":"Bob","age":30}
"#,
    )
    .unwrap();

    let (cli_ctx, catalog_handle, _service_handle) =
        create_cli_context_with_catalog(&temp_dir).await;

    let sql = format!(
        "CREATE EXTERNAL TABLE test_json STORED AS JSON LOCATION '{}'",
        json_path.to_str().unwrap()
    );

    let plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(plan).await.unwrap();

    // Check that statistics were NOT auto-computed (JSON disabled by default)
    let stats = catalog_handle
        .get_table_statistics_manual(None, "test_json")
        .await
        .unwrap();

    assert!(
        stats.is_none(),
        "Statistics should NOT be auto-computed for JSON by default"
    );
}

#[tokio::test]
async fn test_auto_stats_multiple_tables() {
    // Auto-stats should work correctly for multiple tables
    let temp_dir = TempDir::new().unwrap();

    let (cli_ctx, catalog_handle, _service_handle) =
        create_cli_context_with_catalog(&temp_dir).await;

    // Create multiple Parquet tables with different row counts
    let tables = vec![("users", 100), ("orders", 250), ("products", 50)];

    for (name, rows) in &tables {
        let parquet_path = temp_dir.path().join(format!("{}.parquet", name));
        create_test_parquet_file(parquet_path.to_str().unwrap(), *rows).await;

        let sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS PARQUET LOCATION '{}'",
            name,
            parquet_path.to_str().unwrap()
        );

        let plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(plan).await.unwrap();
    }

    // Verify all tables have correct statistics
    for (name, expected_rows) in &tables {
        let stats = catalog_handle
            .get_table_statistics_manual(None, name)
            .await
            .unwrap();
        assert!(
            stats.is_some(),
            "Statistics should exist for table {}",
            name
        );
        assert_eq!(
            stats.unwrap().row_count,
            *expected_rows,
            "Row count for {} should be {}",
            name,
            expected_rows
        );
    }
}

#[tokio::test]
async fn test_column_statistics_extraction() {
    // Verify that column-level statistics are extracted from Parquet
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("test_column_stats.parquet");

    // Create test data with known min/max/null values
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create data with predictable statistics:
    // - id: 1 to 100 (min=1, max=100, no nulls)
    // - age: 20 to 69 (min=20, max=69, no nulls)
    // - score: 0.0 to 99.0 (min=0.0, max=99.0, no nulls)
    let mut ids = Vec::new();
    let mut ages = Vec::new();
    let mut scores = Vec::new();

    for i in 0..100 {
        ids.push(i as i32 + 1); // 1 to 100
        ages.push(20 + (i % 50) as i32); // 20 to 69
        scores.push(i as f64); // 0.0 to 99.0
    }

    let batch = datafusion::arrow::record_batch::RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(datafusion::arrow::array::Int32Array::from(ids))
                as Arc<dyn datafusion::arrow::array::Array>,
        ),
        (
            "age",
            Arc::new(datafusion::arrow::array::Int32Array::from(ages))
                as Arc<dyn datafusion::arrow::array::Array>,
        ),
        (
            "score",
            Arc::new(datafusion::arrow::array::Float64Array::from(scores))
                as Arc<dyn datafusion::arrow::array::Array>,
        ),
    ])
    .unwrap();

    let df = ctx.read_batch(batch).unwrap();
    df.write_parquet(
        parquet_path.to_str().unwrap(),
        datafusion::dataframe::DataFrameWriteOptions::new(),
        None,
    )
    .await
    .unwrap();

    // Create table and verify statistics
    let (cli_ctx, catalog_handle, _service_handle) =
        create_cli_context_with_catalog(&temp_dir).await;

    let sql = format!(
        "CREATE EXTERNAL TABLE test_column_stats STORED AS PARQUET LOCATION '{}'",
        parquet_path.to_str().unwrap()
    );

    let plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(plan).await.unwrap();

    let stats = catalog_handle
        .get_table_statistics_manual(None, "test_column_stats")
        .await
        .unwrap()
        .expect("Statistics should be auto-computed");

    // Verify table-level statistics
    assert_eq!(stats.row_count, 100, "Row count should be 100");
    assert!(stats.size_bytes.is_some(), "File size should be captured");
    assert!(
        stats.size_bytes.unwrap() > 0,
        "File size should be greater than 0"
    );

    // Verify we have column statistics
    assert_eq!(
        stats.column_statistics.len(),
        3,
        "Should have 3 columns: id, age, score"
    );

    // Find each column and verify its statistics
    for col_stat in &stats.column_statistics {
        match col_stat.name.as_str() {
            "id" => {
                assert!(
                    col_stat.min_value.is_some(),
                    "id column should have min_value"
                );
                assert!(
                    col_stat.max_value.is_some(),
                    "id column should have max_value"
                );
                assert_eq!(
                    col_stat.min_value.as_ref().unwrap(),
                    "1",
                    "id min should be 1"
                );
                assert_eq!(
                    col_stat.max_value.as_ref().unwrap(),
                    "100",
                    "id max should be 100"
                );
                assert_eq!(col_stat.null_count, Some(0), "id should have 0 nulls");
            }
            "age" => {
                assert!(
                    col_stat.min_value.is_some(),
                    "age column should have min_value"
                );
                assert!(
                    col_stat.max_value.is_some(),
                    "age column should have max_value"
                );
                assert_eq!(
                    col_stat.min_value.as_ref().unwrap(),
                    "20",
                    "age min should be 20"
                );
                assert_eq!(
                    col_stat.max_value.as_ref().unwrap(),
                    "69",
                    "age max should be 69"
                );
                assert_eq!(col_stat.null_count, Some(0), "age should have 0 nulls");
            }
            "score" => {
                assert!(
                    col_stat.min_value.is_some(),
                    "score column should have min_value"
                );
                assert!(
                    col_stat.max_value.is_some(),
                    "score column should have max_value"
                );
                // Float comparisons: parse and compare numerically
                let min_val: f64 = col_stat.min_value.as_ref().unwrap().parse().unwrap();
                let max_val: f64 = col_stat.max_value.as_ref().unwrap().parse().unwrap();
                assert!(
                    (min_val - 0.0).abs() < 0.01,
                    "score min should be ~0.0, got {}",
                    min_val
                );
                assert!(
                    (max_val - 99.0).abs() < 0.01,
                    "score max should be ~99.0, got {}",
                    max_val
                );
                assert_eq!(col_stat.null_count, Some(0), "score should have 0 nulls");
            }
            _ => panic!("Unexpected column: {}", col_stat.name),
        }
    }

    println!("✅ Column statistics successfully extracted for all columns!");
}
