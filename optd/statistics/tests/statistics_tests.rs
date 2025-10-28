use optd_statistics::{DuckLakeStatisticsProvider, StatisticsProvider};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

// Counter to ensure unique database names
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn create_test_statistics_provider(for_file: bool) -> (TempDir, DuckLakeStatisticsProvider) {
    // Create a unique subdirectory to separate DuckLake metadata for each test
    let temp_dir = TempDir::new().unwrap();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_dir = temp_dir
        .path()
        .join(format!("db_{}_{}", timestamp, counter));
    std::fs::create_dir_all(&unique_dir).unwrap();
    let metadata_path = unique_dir.join("metadata.ducklake");
    if !for_file {
        let provider =
            DuckLakeStatisticsProvider::try_new(None, Some(metadata_path.to_str().unwrap()))
                .unwrap();
        (temp_dir, provider)
    } else {
        let db_path = unique_dir.join("test.db");
        let provider = DuckLakeStatisticsProvider::try_new(
            Some(db_path.to_str().unwrap()),
            Some(metadata_path.to_str().unwrap()),
        )
        .unwrap();
        (temp_dir, provider)
    }
}

#[test]
fn test_ducklake_statistics_provider_creation() {
    {
        // Test memory-based provider
        let _memory_provider = create_test_statistics_provider(false);
        // The provider creation is already asserted in create_test_provider
    }

    {
        // Test file-based provider with unique temporary database
        let (_temp_dir, _provider) = create_test_statistics_provider(true);
        // The provider creation is already asserted in create_test_provider
    }
}

#[test]
fn test_table_stats_insertion() {
    let (_temp_dir, provider) = create_test_statistics_provider(true);

    // Insert table statistics
    let result = provider.update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#);
    match &result {
        Ok(_) => println!("Table stats insertion successful"),
        Err(e) => println!("Table stats insertion failed: {}", e),
    }
    assert!(result.is_ok());
}

#[test]
fn test_table_stats_insertion_and_retrieval() {
    let (_temp_dir, provider) = create_test_statistics_provider(true);

    // Insert table statistics
    let result = provider.update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#);
    match &result {
        Ok(_) => println!("Table stats insertion successful"),
        Err(e) => println!("Table stats insertion failed: {}", e),
    }
    assert!(result.is_ok());

    // Note: Actual retrieval would require setting up the table_metadata
    // TODO
}

#[test]
fn test_fetch_current_schema() {
    let (_temp_dir, provider) = create_test_statistics_provider(true);

    // Fetch the current schema
    let result = provider.fetch_current_schema();

    // Print error if it fails
    if let Err(ref e) = result {
        println!("Error fetching current schema: {}", e);
    }

    // The result should be Ok since DuckLake creates a default 'main' schema
    assert!(
        result.is_ok(),
        "Expected fetch_current_schema to succeed, got error: {:?}",
        result.err()
    );

    let schema = result.unwrap();

    // Verify the schema has valid snapshot information
    println!(
        "Schema name: {}, Schema ID: {}, Begin snapshot: {}, End snapshot: {:?}",
        schema.schema_name, schema.schema_id, schema.begin_snapshot, schema.end_snapshot
    );

    // The schema should have a begin_snapshot value (0 for initial schema in DuckLake)
    assert_eq!(
        schema.schema_name, "main",
        "Expected default schema to be 'main'"
    );
    assert_eq!(
        schema.schema_id, 0,
        "Expected schema_id to be 0 for default schema"
    );
    assert!(
        schema.begin_snapshot >= 0,
        "Schema should have a valid begin_snapshot"
    );

    // End snapshot should be None for current active schema
    assert!(
        schema.end_snapshot.is_none(),
        "Current schema should have no end_snapshot (should be None)"
    );
}

#[test]
fn test_snapshot_versioning_and_stats_types() {
    let (_temp_dir, provider) = create_test_statistics_provider(true);
    let conn = provider.get_connection();

    // Test 1: Multiple columns with sequential snapshots
    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#)
        .unwrap();
    provider
        .update_table_column_stats(2, 1, "ndv", r#"{"distinct_count": 2000}"#)
        .unwrap();
    provider
        .update_table_column_stats(3, 1, "histogram", r#"{"buckets": [1,2,3]}"#)
        .unwrap();

    // Verify different columns have sequential snapshots
    let mut stmt = conn
        .prepare(
            r#"
                SELECT column_id, begin_snapshot
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = 1
                ORDER BY begin_snapshot;
            "#,
        )
        .unwrap();
    let snapshots: Vec<(i64, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(snapshots.len(), 3);
    assert!(snapshots[1].1 > snapshots[0].1);
    assert!(snapshots[2].1 > snapshots[1].1);

    // Test 2: Update same column multiple times - verify snapshot continuity
    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1500}"#)
        .unwrap();
    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 2000}"#)
        .unwrap();

    let mut version_stmt = conn
        .prepare(
            r#"
                SELECT begin_snapshot, end_snapshot, payload
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = 1 AND column_id = 1 AND stats_type = 'ndv'
                ORDER BY begin_snapshot;
            "#,
        )
        .unwrap();
    let versions: Vec<(i64, Option<i64>, String)> = version_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Should have 3 versions (original + 2 updates)
    assert_eq!(versions.len(), 3);

    // First two closed, last one current
    assert!(versions[0].1.is_some());
    assert!(versions[1].1.is_some());
    assert!(versions[2].1.is_none());

    // Verify snapshot continuity
    assert_eq!(versions[0].1.unwrap() + 1, versions[1].0);
    assert_eq!(versions[1].1.unwrap() + 1, versions[2].0);

    // Verify payloads updated correctly
    assert!(versions[0].2.contains("1000"));
    assert!(versions[1].2.contains("1500"));
    assert!(versions[2].2.contains("2000"));

    // Test 3: Multiple stat types for same column coexist
    provider
        .update_table_column_stats(1, 1, "histogram", r#"{"buckets": [1,2,3,4,5]}"#)
        .unwrap();
    provider
        .update_table_column_stats(1, 1, "minmax", r#"{"min": 0, "max": 100}"#)
        .unwrap();

    let type_count: i64 = conn
        .query_row(
            r#"
                SELECT COUNT(DISTINCT stats_type)
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = 1 AND column_id = 1 AND end_snapshot IS NULL
                "#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(type_count, 3); // ndv, histogram, minmax
}

#[test]
fn test_snapshot_tracking_and_multi_table_stats() {
    let (_temp_dir, provider) = create_test_statistics_provider(true);
    let conn = provider.get_connection();

    // Get initial snapshot count
    let initial_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_snapshot",
            [],
            |row| row.get(0),
        )
        .unwrap();

    // Test 1: Snapshot creation tracking - insert stats for 3 columns
    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#)
        .unwrap();
    provider
        .update_table_column_stats(2, 1, "ndv", r#"{"distinct_count": 2000}"#)
        .unwrap();
    provider
        .update_table_column_stats(3, 1, "ndv", r#"{"distinct_count": 3000}"#)
        .unwrap();

    let after_table1_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_snapshot",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_table1_count - initial_count, 3);

    // Verify snapshot_changes were recorded
    let changes_count: i64 = conn
        .query_row(
            r#"
                SELECT COUNT(*) 
                FROM __ducklake_metadata_metalake.main.ducklake_snapshot_changes
                WHERE changes_made LIKE 'updated_stats:%'
                "#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(changes_count, 3);

    // Test 2: Multiple tables with independent tracking
    provider
        .update_table_column_stats(1, 2, "ndv", r#"{"distinct_count": 5000}"#)
        .unwrap();
    provider
        .update_table_column_stats(2, 2, "ndv", r#"{"distinct_count": 6000}"#)
        .unwrap();

    // Verify each table has correct number of stats
    let table1_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats WHERE table_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let table2_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats WHERE table_id = 2",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(table1_count, 3); // 3 columns from table 1
    assert_eq!(table2_count, 2); // 2 columns from table 2

    // Verify all snapshots are sequential across tables
    let mut snapshot_stmt = conn
        .prepare(
            r#"
                SELECT table_id, column_id, begin_snapshot
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                ORDER BY begin_snapshot
                "#,
        )
        .unwrap();
    let all_snapshots: Vec<i64> = snapshot_stmt
        .query_map([], |row| row.get(2))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // All 5 snapshots should be increasing
    for i in 1..all_snapshots.len() {
        assert!(all_snapshots[i] > all_snapshots[i - 1]);
    }
}

/// Helper function to create a test provider with sample table data
fn create_test_provider_with_data() -> (TempDir, DuckLakeStatisticsProvider, i64, i64) {
    let (_temp_dir, provider) = create_test_statistics_provider(false);
    let conn = provider.get_connection();

    // Create a sample table with data
    conn.execute_batch(
        r#"
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            age INTEGER
        );
        
        INSERT INTO test_table VALUES 
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35);
        "#,
    )
    .unwrap();

    // Get table_id and column_ids
    let mut table_id_stmt = conn
        .prepare(
            r#"
            SELECT table_id 
            FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'test_table';
            "#,
        )
        .unwrap();
    let table_id: i64 = table_id_stmt.query_row([], |row| row.get(0)).unwrap();

    // Get the column_id for 'age' column (we'll update stats for this)
    let mut column_id_stmt = conn
        .prepare(
            r#"
            SELECT column_id 
            FROM __ducklake_metadata_metalake.main.ducklake_column
            WHERE table_id = ? AND column_name = 'age';
            "#,
        )
        .unwrap();
    let age_column_id: i64 = column_id_stmt
        .query_row([table_id], |row| row.get(0))
        .unwrap();

    (_temp_dir, provider, table_id, age_column_id)
}

#[test]
fn test_update_and_fetch_table_column_stats() {
    let (_temp_dir, provider, table_id, age_column_id) = create_test_provider_with_data();
    let conn = provider.get_connection();

    // Get initial snapshot
    let initial_snapshot = provider.fetch_current_snapshot().unwrap();
    println!("Initial snapshot ID: {}", initial_snapshot.0);

    // Fetch initial statistics (should have default values from table creation)
    let initial_stats = provider
        .fetch_table_statistics("test_table", initial_snapshot.0, conn)
        .unwrap();
    assert!(initial_stats.is_some());

    // Update min_value for age column
    provider
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .unwrap();

    let snapshot_after_min = provider.fetch_current_snapshot().unwrap();
    assert_eq!(snapshot_after_min.0, initial_snapshot.0 + 1);

    // Update max_value for age column
    provider
        .update_table_column_stats(age_column_id, table_id, "max_value", "35")
        .unwrap();

    let snapshot_after_max = provider.fetch_current_snapshot().unwrap();
    assert_eq!(snapshot_after_max.0, initial_snapshot.0 + 2);

    // Verify the regular column stats were updated
    let mut verify_stmt = conn
        .prepare(
            r#"
            SELECT min_value, max_value
            FROM __ducklake_metadata_metalake.main.ducklake_table_column_stats
            WHERE table_id = ? AND column_id = ?;
            "#,
        )
        .unwrap();

    let (min_val, max_val): (Option<String>, Option<String>) = verify_stmt
        .query_row([table_id, age_column_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();

    assert_eq!(min_val, Some("25".to_string()));
    assert_eq!(max_val, Some("35".to_string()));

    // Verify advanced stats were also created in ducklake_table_column_adv_stats
    let mut adv_stats_stmt = conn
        .prepare(
            r#"
            SELECT stats_type, payload, begin_snapshot, end_snapshot
            FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
            WHERE table_id = ? AND column_id = ?
            ORDER BY stats_type, begin_snapshot;
            "#,
        )
        .unwrap();

    let adv_stats: Vec<(String, String, i64, Option<i64>)> = adv_stats_stmt
        .query_map([table_id, age_column_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(adv_stats.len(), 2);
    assert_eq!(adv_stats[0].0, "max_value");
    assert_eq!(adv_stats[1].0, "min_value");
    assert_eq!(adv_stats[0].1, "35");
    assert_eq!(adv_stats[1].1, "25");
    assert_eq!(adv_stats[0].2, initial_snapshot.0 + 2);
    assert_eq!(adv_stats[1].2, initial_snapshot.0 + 1);
    assert!(adv_stats[0].3.is_none());
    assert!(adv_stats[1].3.is_none());

    let max_value_entry = adv_stats
        .iter()
        .find(|(stats_type, _, _, _)| stats_type == "max_value")
        .expect("max_value entry should exist");
    assert_eq!(max_value_entry.1, "35");
    assert_eq!(max_value_entry.2, initial_snapshot.0 + 2);
    assert!(max_value_entry.3.is_none());

    let min_value_entry = adv_stats
        .iter()
        .find(|(stats_type, _, _, _)| stats_type == "min_value")
        .expect("min_value entry should exist");
    assert_eq!(min_value_entry.1, "25");
    assert_eq!(min_value_entry.2, initial_snapshot.0 + 1);
    assert!(min_value_entry.3.is_none());

    // Test updating an advanced stat type (histogram)
    let histogram_data = json!({
        "buckets": [
            {"min": 20, "max": 30, "count": 2},
            {"min": 30, "max": 40, "count": 1}
        ]
    });

    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            &histogram_data.to_string(),
        )
        .unwrap();

    let snapshot_after_histogram = provider.fetch_current_snapshot().unwrap();
    assert_eq!(snapshot_after_histogram.0, initial_snapshot.0 + 3);

    // Verify histogram was added to advanced stats
    let mut histogram_stmt = conn
        .prepare(
            r#"
            SELECT payload
            FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
            WHERE table_id = ? AND column_id = ? AND stats_type = 'histogram' AND end_snapshot IS NULL;
            "#,
        )
        .unwrap();

    let histogram_payload: String = histogram_stmt
        .query_row([table_id, age_column_id], |row| row.get(0))
        .unwrap();

    assert_eq!(histogram_payload, histogram_data.to_string());

    println!("✓ All update and fetch operations completed successfully");
    println!("  - Initial snapshot: {}", initial_snapshot.0);
    println!("  - After min_value update: {}", snapshot_after_min.0);
    println!("  - After max_value update: {}", snapshot_after_max.0);
    println!("  - After histogram update: {}", snapshot_after_histogram.0);
}
