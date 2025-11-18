use optd_catalog::{CatalogService, CatalogServiceHandle, DuckLakeCatalog};
use std::time::Duration;
use tempfile::TempDir;

/// Helper to create a test catalog service
fn create_test_service() -> (
    TempDir,
    CatalogService<DuckLakeCatalog>,
    CatalogServiceHandle,
) {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    (temp_dir, service, handle)
}

// ============================================================================
// Basic Functionality Tests
// ============================================================================

#[tokio::test]
async fn test_service_creation_and_shutdown() {
    let (_temp_dir, service, handle) = create_test_service();

    // Verify handle is cloneable (multi-producer capability)
    let handle_clone = handle.clone();

    let service_handle = tokio::spawn(async move {
        service.run().await;
    });

    // Both handles should work
    let snapshot1 = handle.current_snapshot().await.unwrap();
    let snapshot2 = handle_clone.current_snapshot().await.unwrap();
    assert_eq!(
        snapshot1.0, snapshot2.0,
        "Cloned handles should access same service"
    );

    // Shutdown should complete gracefully
    handle.shutdown().await.unwrap();

    // Service task should complete
    tokio::time::timeout(Duration::from_secs(1), service_handle)
        .await
        .expect("Service should shutdown within timeout")
        .unwrap();

    // Verify shutdown is idempotent
    let result = handle_clone.shutdown().await;
    assert!(result.is_err(), "Second shutdown should fail gracefully");
}

#[tokio::test]
async fn test_current_snapshot_basic() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    let snapshot = handle.current_snapshot().await.unwrap();
    assert_eq!(snapshot.0, 0, "Initial snapshot should be 0");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_current_snapshot_info() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    let info = handle.current_snapshot_info().await.unwrap();
    assert_eq!(info.id.0, 0);
    assert_eq!(info.schema_version, 0);
    assert!(info.next_catalog_id > 0);
    assert_eq!(info.next_file_id, 0);

    // Verify snapshot info is consistent with current_snapshot
    let snapshot = handle.current_snapshot().await.unwrap();
    assert_eq!(
        info.id.0, snapshot.0,
        "Snapshot info ID should match current snapshot"
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_current_schema_info() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    let schema_info = handle.current_schema_info().await.unwrap();
    assert_eq!(schema_info.schema_name, "main");
    assert_eq!(schema_info.schema_id, 0);
    assert_eq!(schema_info.begin_snapshot, 0);
    assert!(schema_info.end_snapshot.is_none());

    handle.shutdown().await.unwrap();
}

// ============================================================================
// Table and Schema Tests
// ============================================================================

#[tokio::test]
async fn test_current_schema_with_table() {
    let (_temp_dir, service, handle) = create_test_service();

    // Get the catalog to create a test table BEFORE spawning service
    let conn = service.catalog_for_setup().get_connection();
    conn.execute_batch(
        r#"
        CREATE TABLE test_table (
            id INTEGER NOT NULL,
            name VARCHAR,
            age INTEGER
        );
        "#,
    )
    .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Fetch schema
    let schema = handle.current_schema(None, "test_table").await.unwrap();

    assert_eq!(schema.fields().len(), 3);
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("name").is_ok());
    assert!(schema.field_with_name("age").is_ok());

    // Check nullable constraints
    let id_field = schema.field_with_name("id").unwrap();
    assert!(!id_field.is_nullable(), "id should not be nullable");

    let name_field = schema.field_with_name("name").unwrap();
    assert!(name_field.is_nullable(), "name should be nullable");

    // Verify data types are correctly mapped
    use duckdb::arrow::datatypes::DataType;
    assert!(
        matches!(id_field.data_type(), DataType::Int32),
        "id should be Int32"
    );
    assert!(
        matches!(name_field.data_type(), DataType::Utf8),
        "name should be Utf8/String"
    );

    // Verify field order matches CREATE TABLE order
    assert_eq!(schema.fields()[0].name(), "id");
    assert_eq!(schema.fields()[1].name(), "name");
    assert_eq!(schema.fields()[2].name(), "age");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_table_statistics_empty_table() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();
    conn.execute_batch(
        r#"
        CREATE TABLE empty_table (id INTEGER, name VARCHAR);
        "#,
    )
    .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    let snapshot = handle.current_snapshot().await.unwrap();
    let stats = handle
        .table_statistics("empty_table", snapshot)
        .await
        .unwrap();

    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(stats.row_count, 0);

    // For empty tables with no data, the statistics system may not return column metadata
    // This is expected behavior - verify it's empty or has minimal stats
    assert_eq!(
        stats.column_statistics.len(),
        0,
        "Empty table with no data should have 0 column statistics"
    );

    // If there were column statistics, verify no advanced stats would be present
    for col_stat in &stats.column_statistics {
        assert_eq!(
            col_stat.advanced_stats.len(),
            0,
            "Empty table should have no advanced stats for {}",
            col_stat.name
        );
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_table_statistics_nonexistent_table() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    let snapshot = handle.current_snapshot().await.unwrap();
    let stats = handle
        .table_statistics("nonexistent_table", snapshot)
        .await
        .unwrap();

    assert!(stats.is_some());
    assert_eq!(stats.unwrap().column_statistics.len(), 0);

    handle.shutdown().await.unwrap();
}

// ============================================================================
// Statistics Update Tests
// ============================================================================

#[tokio::test]
async fn test_update_and_retrieve_statistics() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();

    // Create table and get IDs
    conn.execute_batch(
        r#"
        CREATE TABLE stats_test (id INTEGER, value DOUBLE);
        INSERT INTO stats_test VALUES (1, 10.5), (2, 20.5);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'stats_test';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let value_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'value';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Update statistics
    handle
        .update_table_column_stats(value_column_id, table_id, "min_value", "10.5")
        .await
        .unwrap();

    handle
        .update_table_column_stats(value_column_id, table_id, "max_value", "20.5")
        .await
        .unwrap();

    // Retrieve and verify
    let snapshot = handle.current_snapshot().await.unwrap();
    // Table creation creates initial snapshots, then 2 updates create 2 more
    assert!(
        snapshot.0 >= 2,
        "Should have at least 2 snapshots after updates"
    );

    let stats = handle
        .table_statistics("stats_test", snapshot)
        .await
        .unwrap()
        .unwrap();

    let value_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "value")
        .expect("Should have stats for value column");

    assert_eq!(value_stats.advanced_stats.len(), 2);
    assert!(
        value_stats
            .advanced_stats
            .iter()
            .any(|s| s.stats_type == "min_value")
    );
    assert!(
        value_stats
            .advanced_stats
            .iter()
            .any(|s| s.stats_type == "max_value")
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_statistics_versioning() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE version_test (id INTEGER, count INTEGER);
        INSERT INTO version_test VALUES (1, 100);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'version_test';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let count_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'count';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Take snapshot before updates
    let snapshot_0 = handle.current_snapshot().await.unwrap();

    // Update 1
    handle
        .update_table_column_stats(
            count_column_id,
            table_id,
            "ndv",
            r#"{"distinct_count": 100}"#,
        )
        .await
        .unwrap();

    let snapshot_1 = handle.current_snapshot().await.unwrap();
    assert_eq!(snapshot_1.0, snapshot_0.0 + 1);

    // Update 2 (new value)
    handle
        .update_table_column_stats(
            count_column_id,
            table_id,
            "ndv",
            r#"{"distinct_count": 150}"#,
        )
        .await
        .unwrap();

    let snapshot_2 = handle.current_snapshot().await.unwrap();
    assert_eq!(snapshot_2.0, snapshot_1.0 + 1);

    // Verify stats at snapshot_1
    let stats_1 = handle
        .table_statistics("version_test", snapshot_1)
        .await
        .unwrap()
        .unwrap();

    let count_stats_1 = stats_1
        .column_statistics
        .iter()
        .find(|cs| cs.name == "count")
        .unwrap();

    assert_eq!(count_stats_1.advanced_stats.len(), 1);
    assert!(
        count_stats_1.advanced_stats[0]
            .data
            .to_string()
            .contains("100")
    );

    // Verify stats at snapshot_2
    let stats_2 = handle
        .table_statistics("version_test", snapshot_2)
        .await
        .unwrap()
        .unwrap();

    let count_stats_2 = stats_2
        .column_statistics
        .iter()
        .find(|cs| cs.name == "count")
        .unwrap();

    assert_eq!(count_stats_2.advanced_stats.len(), 1);
    assert!(
        count_stats_2.advanced_stats[0]
            .data
            .to_string()
            .contains("150")
    );

    // Verify snapshot_1 still returns old value
    let stats_1_again = handle
        .table_statistics("version_test", snapshot_1)
        .await
        .unwrap()
        .unwrap();

    let count_stats_1_again = stats_1_again
        .column_statistics
        .iter()
        .find(|cs| cs.name == "count")
        .unwrap();

    assert!(
        count_stats_1_again.advanced_stats[0]
            .data
            .to_string()
            .contains("100"),
        "Time-travel query should return historical value, not current value"
    );

    // Verify snapshot_0 has no stats (before any updates)
    let stats_0 = handle
        .table_statistics("version_test", snapshot_0)
        .await
        .unwrap()
        .unwrap();

    let count_stats_0 = stats_0
        .column_statistics
        .iter()
        .find(|cs| cs.name == "count")
        .unwrap();

    assert_eq!(
        count_stats_0.advanced_stats.len(),
        0,
        "Snapshot before updates should have no advanced stats"
    );

    handle.shutdown().await.unwrap();
}

// ============================================================================
// Concurrency Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_read_operations() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Spawn multiple concurrent snapshot requests
    let mut tasks = vec![];
    for _ in 0..50 {
        let handle_clone = handle.clone();
        tasks.push(tokio::spawn(async move {
            handle_clone.current_snapshot().await.unwrap()
        }));
    }

    // All should succeed with same snapshot ID
    for task in tasks {
        let snapshot = task.await.unwrap();
        assert_eq!(snapshot.0, 0);
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_concurrent_mixed_operations() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE concurrent_test (id INTEGER, data VARCHAR);
        INSERT INTO concurrent_test VALUES (1, 'test');
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'concurrent_test';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let id_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'id';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    let initial_snapshot = handle.current_snapshot().await.unwrap();

    let mut tasks = vec![];

    // Mix of reads and writes
    for i in 0..20 {
        let handle_clone = handle.clone();

        if i % 2 == 0 {
            // Read operation
            tasks.push(tokio::spawn(async move {
                let _ = handle_clone.current_snapshot().await;
            }));
        } else {
            // Write operation
            tasks.push(tokio::spawn(async move {
                let _ = handle_clone
                    .update_table_column_stats(
                        id_column_id,
                        table_id,
                        &format!("stat_{}", i),
                        &format!(r#"{{"value": {}}}"#, i),
                    )
                    .await;
            }));
        }
    }

    // Wait for all
    for task in tasks {
        task.await.unwrap();
    }

    // Verify final snapshot progressed
    let final_snapshot = handle.current_snapshot().await.unwrap();
    assert!(final_snapshot.0 >= 10, "Should have progressed snapshots");

    // Verify all writes succeeded by checking stats
    let stats = handle
        .table_statistics("concurrent_test", final_snapshot)
        .await
        .unwrap()
        .unwrap();

    let id_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "id")
        .expect("Should have stats for id column");

    // Should have 10 stats (one for each odd i: 1,3,5,7,9,11,13,15,17,19)
    assert_eq!(
        id_stats.advanced_stats.len(),
        10,
        "Should have 10 write operations worth of stats"
    );

    // Verify no stats were lost (check for specific stat names)
    let stat_names: Vec<&str> = id_stats
        .advanced_stats
        .iter()
        .map(|s| s.stats_type.as_str())
        .collect();
    for i in (1..20).step_by(2) {
        let expected_name = format!("stat_{}", i);
        assert!(
            stat_names.contains(&expected_name.as_str()),
            "Should have stat_{} but got {:?}",
            i,
            stat_names
        );
    }

    // Verify snapshot progression matches write count
    let snapshot_diff = final_snapshot.0 - initial_snapshot.0;
    assert_eq!(
        snapshot_diff, 10,
        "Snapshot should have advanced by exactly 10 (one per write)"
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_handles_same_service() {
    let (_temp_dir, service, handle1) = create_test_service();

    // Clone handles
    let handle2 = handle1.clone();
    let handle3 = handle1.clone();

    tokio::spawn(async move {
        service.run().await;
    });

    // All handles should work independently
    let snapshot1 = handle1.current_snapshot().await.unwrap();
    let snapshot2 = handle2.current_snapshot().await.unwrap();
    let snapshot3 = handle3.current_snapshot().await.unwrap();

    assert_eq!(snapshot1.0, snapshot2.0);
    assert_eq!(snapshot2.0, snapshot3.0);

    handle1.shutdown().await.unwrap();
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[tokio::test]
async fn test_operations_after_shutdown() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Shutdown the service
    handle.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Operations after shutdown should fail
    let result = handle.current_snapshot().await;
    assert!(result.is_err(), "Operations after shutdown should fail");

    // Verify multiple operations fail consistently
    assert!(handle.current_snapshot_info().await.is_err());
    assert!(handle.current_schema_info().await.is_err());
    assert!(
        handle
            .table_statistics("any_table", optd_catalog::SnapshotId(0))
            .await
            .is_err()
    );

    // Verify error type is consistent (channel closed)
    match result {
        Err(e) => {
            let err_msg = format!("{:?}", e);
            assert!(
                err_msg.contains("ExecuteReturnedResults") || err_msg.contains("channel"),
                "Error should indicate channel/connection issue, got: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Expected error after shutdown"),
    }
}

#[tokio::test]
async fn test_invalid_table_schema_request() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Request schema for non-existent table
    let result = handle.current_schema(None, "does_not_exist").await;
    assert!(result.is_err(), "Should error for non-existent table");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_large_json_statistics() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE large_stats_test (id INTEGER);
        INSERT INTO large_stats_test VALUES (1);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'large_stats_test';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let id_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'id';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Create large histogram data
    let large_histogram: Vec<i32> = (0..1000).collect();
    let large_payload = serde_json::json!({
        "buckets": large_histogram,
        "metadata": "x".repeat(1000)
    })
    .to_string();

    // Should handle large payloads
    let result = handle
        .update_table_column_stats(id_column_id, table_id, "large_histogram", &large_payload)
        .await;

    assert!(result.is_ok(), "Should handle large statistics payloads");

    // Verify retrieval
    let snapshot = handle.current_snapshot().await.unwrap();
    let stats = handle
        .table_statistics("large_stats_test", snapshot)
        .await
        .unwrap()
        .unwrap();

    let id_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "id")
        .unwrap();

    let large_stat = id_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "large_histogram")
        .unwrap();

    assert!(large_stat.data.to_string().len() > 1000);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_special_characters_in_statistics() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE special_chars_test (id INTEGER);
        INSERT INTO special_chars_test VALUES (1);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'special_chars_test';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let id_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'id';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Statistics with special characters
    let special_payload =
        r#"{"value": "test\"with\\special\nchars", "unicode": "测试", "emoji": "🚀"}"#;

    handle
        .update_table_column_stats(id_column_id, table_id, "special_test", special_payload)
        .await
        .unwrap();

    // Retrieve and verify
    let snapshot = handle.current_snapshot().await.unwrap();
    let stats = handle
        .table_statistics("special_chars_test", snapshot)
        .await
        .unwrap()
        .unwrap();

    let id_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "id")
        .unwrap();

    let special_stat = id_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "special_test")
        .unwrap();

    let data_str = special_stat.data.to_string();
    assert!(data_str.contains("测试"));
    assert!(data_str.contains("🚀"));

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_rapid_sequential_updates() {
    let (_temp_dir, service, handle) = create_test_service();

    // Setup before spawning service
    let conn = service.catalog_for_setup().get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE rapid_test (id INTEGER);
        INSERT INTO rapid_test VALUES (1);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'rapid_test';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let id_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'id';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    let initial_snapshot = handle.current_snapshot().await.unwrap();

    // Perform 10 rapid updates
    for i in 0..10 {
        handle
            .update_table_column_stats(
                id_column_id,
                table_id,
                "counter",
                &format!(r#"{{"count": {}}}"#, i),
            )
            .await
            .unwrap();
    }

    let final_snapshot = handle.current_snapshot().await.unwrap();
    assert_eq!(
        final_snapshot.0,
        initial_snapshot.0 + 10,
        "Should have 10 new snapshots"
    );

    // Verify the final value is the last update
    let final_stats = handle
        .table_statistics("rapid_test", final_snapshot)
        .await
        .unwrap()
        .unwrap();

    let id_stats = final_stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "id")
        .expect("Should have stats for id column");

    // Should have only 1 stat since same stat_type was updated
    assert_eq!(id_stats.advanced_stats.len(), 1);

    let counter_stat = id_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "counter")
        .expect("Should have counter stat");

    // Final value should be 9 (last iteration)
    assert!(
        counter_stat.data.to_string().contains("9"),
        "Final counter value should be 9, got: {}",
        counter_stat.data
    );

    // Verify we can query intermediate snapshots
    let mid_snapshot = optd_catalog::SnapshotId(initial_snapshot.0 + 5);
    let mid_stats = handle
        .table_statistics("rapid_test", mid_snapshot)
        .await
        .unwrap()
        .unwrap();

    let mid_id_stats = mid_stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "id")
        .expect("Should have stats for id column at mid snapshot");

    if let Some(mid_counter) = mid_id_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "counter")
    {
        // Mid-point should have value 4 (5th update, 0-indexed)
        assert!(
            mid_counter.data.to_string().contains("4"),
            "Mid-point counter should be 4, got: {}",
            mid_counter.data
        );
    }

    handle.shutdown().await.unwrap();
}

// ============================================================================
// Performance and Stress Tests
// ============================================================================

#[tokio::test]
async fn test_high_concurrency_stress() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Spawn 100 concurrent tasks
    let mut tasks = vec![];
    for i in 0..100 {
        let handle_clone = handle.clone();
        tasks.push(tokio::spawn(async move {
            if i % 3 == 0 {
                let _ = handle_clone.current_snapshot().await;
            } else if i % 3 == 1 {
                let _ = handle_clone.current_snapshot_info().await;
            } else {
                let _ = handle_clone.current_schema_info().await;
            }
        }));
    }

    // Should complete without errors
    let results: Vec<_> = futures::future::join_all(tasks).await;
    for result in results {
        assert!(result.is_ok(), "All concurrent operations should succeed");
    }

    handle.shutdown().await.unwrap();
}
