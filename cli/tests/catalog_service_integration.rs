// Integration tests for OptD catalog service handle functions

use datafusion::{
    arrow::array::{Int32Array, RecordBatch},
    arrow::datatypes::{DataType, Field, Schema},
    catalog::CatalogProviderList,
    prelude::SessionContext,
};
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_catalog_service_handle() -> Result<(), Box<dyn std::error::Error>> {
    // Setup catalog with test data
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    {
        let setup_catalog = DuckLakeCatalog::try_new(
            Some(db_path.to_str().unwrap()),
            Some(metadata_path.to_str().unwrap()),
        )?;
        let conn = setup_catalog.get_connection();
        conn.execute_batch("CREATE TABLE test_table (id INTEGER, name VARCHAR, age INTEGER)")?;
        conn.execute_batch(
            "INSERT INTO test_table VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)",
        )?;
    }

    // Start catalog service again to check restart resilience
    let catalog = DuckLakeCatalog::try_new(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )?;
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    // Test catalog service handle functions
    let snapshot = handle.current_snapshot().await?;
    assert_eq!(
        snapshot.0, 2,
        "Snapshot should be 2 (CREATE TABLE and INSERT)"
    );

    let snapshot_info = handle.current_snapshot_info().await?;
    assert!(
        snapshot_info.schema_version >= 0,
        "Schema version should be greater than or equal to 0"
    );
    assert_eq!(snapshot_info.id.0, snapshot.0, "Snapshot IDs should match");

    let schema = handle.current_schema(None, "test_table").await?;
    assert_eq!(schema.fields().len(), 3, "Should have 3 fields");
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "age");

    // Test statistics
    let query_catalog = DuckLakeCatalog::try_new(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )?;
    let conn = query_catalog.get_connection();

    let table_id: i64 = conn.query_row(
        "SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
         INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
         WHERE ds.schema_name = current_schema() AND dt.table_name = 'test_table'",
        [],
        |row| row.get(0),
    )?;

    let age_column_id: i64 = conn.query_row(
        "SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
         WHERE table_id = ? AND column_name = 'age'",
        [table_id],
        |row| row.get(0),
    )?;

    // Test statistics update API
    handle
        .update_table_column_stats(age_column_id, table_id, "ndv", r#"{"distinct_count": 3}"#)
        .await?;

    let updated_snapshot = handle.current_snapshot().await?;
    assert_eq!(
        updated_snapshot.0, 3,
        "Should be snapshot 3 after stats update"
    );

    let stats = handle
        .table_statistics("test_table", updated_snapshot)
        .await?
        .unwrap();
    assert_eq!(stats.row_count, 3, "Should have 3 rows");

    let age_stats = stats
        .column_statistics
        .iter()
        .find(|c| c.name == "age")
        .expect("Should have statistics for 'age' column");

    assert_eq!(age_stats.name, "age");
    assert_eq!(age_stats.column_type, "int32");

    // Verify the ndv statistic was actually persisted
    assert_eq!(
        age_stats.advanced_stats.len(),
        1,
        "Should have 1 advanced statistic"
    );
    assert_eq!(age_stats.advanced_stats[0].stats_type, "ndv");
    assert_eq!(
        age_stats.advanced_stats[0]
            .data
            .get("distinct_count")
            .and_then(|v| v.as_i64()),
        Some(3),
        "Should have distinct_count of 3 in ndv statistic"
    );

    // Test multiple statistics on the same column (add histogram)
    handle
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            r#"{"buckets": [{"lower": 25, "upper": 30, "count": 2}, {"lower": 30, "upper": 35, "count": 1}]}"#
        )
        .await?;

    let updated_snapshot2 = handle.current_snapshot().await?;
    assert_eq!(
        updated_snapshot2.0, 4,
        "Should be snapshot 4 after histogram update"
    );

    let stats2 = handle
        .table_statistics("test_table", updated_snapshot2)
        .await?
        .unwrap();

    let age_stats2 = stats2
        .column_statistics
        .iter()
        .find(|c| c.name == "age")
        .expect("Should have statistics for 'age' column");

    // Should now have both ndv and histogram statistics
    assert_eq!(
        age_stats2.advanced_stats.len(),
        2,
        "Should have 2 advanced statistics"
    );

    let ndv_stat = age_stats2
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "ndv")
        .expect("Should have ndv");
    let histogram_stat = age_stats2
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "histogram")
        .expect("Should have histogram");

    assert_eq!(
        ndv_stat.data.get("distinct_count").and_then(|v| v.as_i64()),
        Some(3),
        "ndv statistic should persist"
    );

    assert!(
        histogram_stat
            .data
            .get("buckets")
            .and_then(|v| v.as_array())
            .is_some(),
        "histogram should have buckets array"
    );

    let buckets = histogram_stat
        .data
        .get("buckets")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(buckets.len(), 2, "Should have 2 histogram buckets");

    Ok(())
}

#[tokio::test]
async fn test_datafusion_catalog_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Setup catalog with test data and statistics
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    {
        let setup_catalog = DuckLakeCatalog::try_new(
            Some(db_path.to_str().unwrap()),
            Some(metadata_path.to_str().unwrap()),
        )?;
        let conn = setup_catalog.get_connection();
        conn.execute_batch("CREATE TABLE df_test (id INTEGER, value INTEGER)")?;
        conn.execute_batch(
            "INSERT INTO df_test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
        )?;
    }

    let catalog = DuckLakeCatalog::try_new(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )?;
    let (service, handle) = CatalogService::new(catalog);
    tokio::spawn(async move { service.run().await });

    // Setup statistics for testing
    let query_catalog = DuckLakeCatalog::try_new(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )?;
    let conn = query_catalog.get_connection();

    let table_id: i64 = conn.query_row(
        "SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
         INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
         WHERE ds.schema_name = current_schema() AND dt.table_name = 'df_test'",
        [],
        |row| row.get(0),
    )?;

    let value_column_id: i64 = conn.query_row(
        "SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
         WHERE table_id = ? AND column_name = 'value'",
        [table_id],
        |row| row.get(0),
    )?;

    // Add test statistics
    handle
        .update_table_column_stats(value_column_id, table_id, "ndv", r#"{"distinct_count": 5}"#)
        .await?;
    handle
        .update_table_column_stats(
            value_column_id,
            table_id,
            "histogram",
            r#"{"buckets": [{"lower": 10, "upper": 30, "count": 3}, {"lower": 30, "upper": 50, "count": 2}]}"#
        )
        .await?;

    // Test DataFusion catalog integration
    let ctx = SessionContext::new();
    ctx.register_batch(
        "df_test",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )?,
    )?;

    let optd_catalog_list =
        OptdCatalogProviderList::new(ctx.state().catalog_list().clone(), Some(handle.clone()));

    let catalog = optd_catalog_list.catalog("datafusion").unwrap();
    let optd_catalog = catalog
        .as_any()
        .downcast_ref::<optd_datafusion::OptdCatalogProvider>()
        .expect("Should be OptdCatalogProvider");

    assert!(
        optd_catalog.catalog_handle().is_some(),
        "Catalog handle should propagate through DataFusion integration"
    );

    // Verify statistics retrieval through DataFusion catalog
    let stats_via_catalog = optd_catalog
        .catalog_handle()
        .unwrap()
        .table_statistics(
            "df_test",
            optd_catalog
                .catalog_handle()
                .unwrap()
                .current_snapshot()
                .await?,
        )
        .await?
        .unwrap();

    assert_eq!(stats_via_catalog.row_count, 5);

    let value_stats = stats_via_catalog
        .column_statistics
        .iter()
        .find(|c| c.name == "value")
        .expect("Should find value column statistics");

    assert_eq!(
        value_stats.advanced_stats.len(),
        2,
        "Should have both ndv and histogram stats"
    );

    // Verify ndv statistic
    assert_eq!(
        value_stats
            .advanced_stats
            .iter()
            .find(|s| s.stats_type == "ndv")
            .and_then(|s| s.data.get("distinct_count").and_then(|v| v.as_i64())),
        Some(5),
        "Should retrieve ndv statistics through DataFusion catalog"
    );

    // Verify histogram statistic
    let histogram = value_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "histogram")
        .expect("Should have histogram statistic");
    let buckets = histogram
        .data
        .get("buckets")
        .and_then(|v| v.as_array())
        .expect("Should have buckets");
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].get("lower").and_then(|v| v.as_i64()), Some(10));
    assert_eq!(buckets[0].get("count").and_then(|v| v.as_i64()), Some(3));

    Ok(())
}
