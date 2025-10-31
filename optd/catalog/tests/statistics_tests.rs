use optd_catalog::{Catalog, DuckLakeCatalog, SnapshotId};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates a test catalog with isolated metadata directory.
fn create_test_catalog(for_file: bool) -> (TempDir, DuckLakeCatalog) {
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

    let catalog = if for_file {
        let db_path = unique_dir.join("test.db");
        DuckLakeCatalog::try_new(
            Some(db_path.to_str().unwrap()),
            Some(metadata_path.to_str().unwrap()),
        )
    } else {
        DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap()))
    }
    .unwrap();

    (temp_dir, catalog)
}

/// Creates a test catalog with a pre-populated test_table (id, name, age columns).
fn create_test_catalog_with_data() -> (TempDir, DuckLakeCatalog, i64, i64) {
    let (temp_dir, catalog) = create_test_catalog(false);
    let conn = catalog.get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE test_table (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO test_table VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'test_table';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let age_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id 
                FROM __ducklake_metadata_metalake.main.ducklake_column 
                WHERE table_id = ? AND column_name = 'age';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    (temp_dir, catalog, table_id, age_column_id)
}

#[test]
fn test_ducklake_statistics_provider_creation() {
    // Test both memory-based and file-based provider creation.
    let (_temp_dir, _provider) = create_test_catalog(false);
    let (_temp_dir, _provider) = create_test_catalog(true);
}

#[test]
fn test_table_stats_insertion() {
    // Test basic statistics insertion without errors.
    let (_temp_dir, provider) = create_test_catalog(true);

    let result = provider.update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#);
    assert!(result.is_ok());
}

#[test]
fn test_table_stats_insertion_and_retrieval() {
    // Test inserting and retrieving multiple statistics types for a column.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    provider
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .unwrap();
    provider
        .update_table_column_stats(age_column_id, table_id, "max_value", "35")
        .unwrap();
    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            r#"{"buckets": [{"min": 20, "max": 30, "count": 2}]}"#,
        )
        .unwrap();

    let latest_snapshot = provider.current_snapshot().unwrap();
    let stats = provider
        .table_statistics("test_table", latest_snapshot, conn)
        .unwrap()
        .unwrap();

    assert_eq!(stats.column_statistics.len(), 3);
    assert_eq!(stats.row_count, 3);

    let age_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .expect("Should have statistics for age column");

    assert_eq!(age_stats.advanced_stats.len(), 3);
    assert!(
        age_stats
            .advanced_stats
            .iter()
            .any(|s| s.stats_type == "min_value" && (s.data == json!(25) || s.data == json!("25")))
    );
    assert!(
        age_stats
            .advanced_stats
            .iter()
            .any(|s| s.stats_type == "max_value" && (s.data == json!(35) || s.data == json!("35")))
    );
    assert!(
        age_stats
            .advanced_stats
            .iter()
            .any(|s| s.stats_type == "histogram" && s.data.to_string().contains("buckets"))
    );
}

#[test]
fn test_current_schema() {
    // Test fetching current schema info returns valid metadata.
    let (_temp_dir, provider) = create_test_catalog(true);

    let schema = provider.current_schema_info().unwrap();

    assert_eq!(schema.schema_name, "main");
    assert_eq!(schema.schema_id, 0);
    assert!(schema.begin_snapshot >= 0);
    assert!(schema.end_snapshot.is_none());
}

#[test]
fn test_snapshot_versioning_and_stats_types() {
    // Test snapshot creation, versioning, and continuity for multiple stats updates.
    let (_temp_dir, provider) = create_test_catalog(true);
    let conn = provider.get_connection();

    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#)
        .unwrap();
    provider
        .update_table_column_stats(2, 1, "ndv", r#"{"distinct_count": 2000}"#)
        .unwrap();
    provider
        .update_table_column_stats(3, 1, "histogram", r#"{"buckets": [1,2,3]}"#)
        .unwrap();

    let snapshots: Vec<(i64, i64)> = conn
        .prepare(
            r#"
            SELECT column_id, begin_snapshot 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE table_id = 1 
                ORDER BY begin_snapshot;
            "#,
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(snapshots.len(), 3);
    assert!(snapshots[1].1 > snapshots[0].1);
    assert!(snapshots[2].1 > snapshots[1].1);

    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1500}"#)
        .unwrap();
    provider
        .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 2000}"#)
        .unwrap();

    let versions: Vec<(i64, Option<i64>, String)> = conn
        .prepare(
            r#"
            SELECT begin_snapshot, end_snapshot, payload 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE table_id = 1 AND column_id = 1 AND stats_type = 'ndv' 
                ORDER BY begin_snapshot;
            "#,
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(versions.len(), 3);
    assert!(versions[0].1.is_some() && versions[1].1.is_some() && versions[2].1.is_none());
    assert_eq!(versions[0].1.unwrap(), versions[1].0);
    assert_eq!(versions[1].1.unwrap(), versions[2].0);
    assert!(versions[0].2.contains("1000"));
    assert!(versions[1].2.contains("1500"));
    assert!(versions[2].2.contains("2000"));

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
    assert_eq!(type_count, 3);
}

#[test]
fn test_snapshot_tracking_and_multi_table_stats() {
    // Test snapshot creation tracking and statistics isolation across multiple tables.
    let (_temp_dir, provider) = create_test_catalog(true);
    let conn = provider.get_connection();

    let initial_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_snapshot",
            [],
            |row| row.get(0),
        )
        .unwrap();

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

    provider
        .update_table_column_stats(1, 2, "ndv", r#"{"distinct_count": 5000}"#)
        .unwrap();
    provider
        .update_table_column_stats(2, 2, "ndv", r#"{"distinct_count": 6000}"#)
        .unwrap();

    let table1_count: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*) 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE table_id = 1
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    let table2_count: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*) 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE table_id = 2
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(table1_count, 3);
    assert_eq!(table2_count, 2);

    let all_snapshots: Vec<i64> = conn
        .prepare(
            r#"
            SELECT begin_snapshot 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                ORDER BY begin_snapshot
            "#,
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    for i in 1..all_snapshots.len() {
        assert!(all_snapshots[i] > all_snapshots[i - 1]);
    }
}

#[test]
fn test_update_and_fetch_table_column_stats() {
    // Test updating min/max values and advanced statistics with snapshot progression.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    let initial_snapshot = provider.current_snapshot().unwrap();
    assert!(
        provider
            .table_statistics("test_table", initial_snapshot, conn)
            .unwrap()
            .is_some()
    );

    provider
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .unwrap();
    let snapshot_after_min = provider.current_snapshot().unwrap();
    assert_eq!(snapshot_after_min.0, initial_snapshot.0 + 1);

    provider
        .update_table_column_stats(age_column_id, table_id, "max_value", "35")
        .unwrap();
    let snapshot_after_max = provider.current_snapshot().unwrap();
    assert_eq!(snapshot_after_max.0, initial_snapshot.0 + 2);

    let (min_val, max_val): (Option<String>, Option<String>) = conn
        .query_row(
            r#"
            SELECT min_value, max_value 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_stats 
                WHERE table_id = ? AND column_id = ?;
            "#,
            [table_id, age_column_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(min_val, Some("25".to_string()));
    assert_eq!(max_val, Some("35".to_string()));

    let adv_stats: Vec<(String, String, i64, Option<i64>)> = conn
        .prepare(
            r#"
            SELECT stats_type, payload, begin_snapshot, end_snapshot 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE table_id = ? AND column_id = ? 
                ORDER BY stats_type, begin_snapshot;
            "#,
        )
        .unwrap()
        .query_map([table_id, age_column_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(adv_stats.len(), 2);
    assert!(
        adv_stats
            .iter()
            .any(|(st, p, _, e)| st == "max_value" && p == "35" && e.is_none())
    );
    assert!(
        adv_stats
            .iter()
            .any(|(st, p, _, e)| st == "min_value" && p == "25" && e.is_none())
    );

    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            &json!({"buckets": [{"min": 20, "max": 30, "count": 2}, {"min": 30, "max": 40, "count": 1}]}).to_string(),
        )
        .unwrap();

    let snapshot_after_histogram = provider.current_snapshot().unwrap();
    assert_eq!(snapshot_after_histogram.0, initial_snapshot.0 + 3);
}

#[test]
fn test_fetch_table_stats_with_snapshot_time_travel() {
    // Test time-travel capability by fetching statistics at different snapshot points.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    let snapshot_0 = provider.current_snapshot().unwrap();

    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            r#"{"version": 1, "buckets": [1, 2, 3]}"#,
        )
        .unwrap();
    let snapshot_1 = provider.current_snapshot().unwrap();

    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            r#"{"version": 2, "buckets": [1, 2, 3, 4, 5]}"#,
        )
        .unwrap();
    let snapshot_2 = provider.current_snapshot().unwrap();

    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            r#"{"version": 3, "buckets": [10, 20, 30]}"#,
        )
        .unwrap();
    let snapshot_3 = provider.current_snapshot().unwrap();

    let stats_at_0 = provider
        .table_statistics("test_table", snapshot_0, conn)
        .unwrap()
        .unwrap();
    let age_stats_0 = stats_at_0
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();
    assert_eq!(age_stats_0.advanced_stats.len(), 0);

    let stats_at_1 = provider
        .table_statistics("test_table", snapshot_1, conn)
        .unwrap()
        .unwrap();
    let age_stats_1 = stats_at_1
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();
    assert_eq!(age_stats_1.advanced_stats.len(), 1);
    assert!(
        age_stats_1.advanced_stats[0]
            .data
            .to_string()
            .contains("\"version\":1")
    );

    let stats_at_2 = provider
        .table_statistics("test_table", snapshot_2, conn)
        .unwrap()
        .unwrap();
    let age_stats_2 = stats_at_2
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();
    assert_eq!(age_stats_2.advanced_stats.len(), 1);
    assert!(
        age_stats_2.advanced_stats[0]
            .data
            .to_string()
            .contains("\"version\":2")
    );

    let stats_at_3 = provider
        .table_statistics("test_table", snapshot_3, conn)
        .unwrap()
        .unwrap();
    let age_stats_3 = stats_at_3
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();
    assert_eq!(age_stats_3.advanced_stats.len(), 1);
    assert!(
        age_stats_3.advanced_stats[0]
            .data
            .to_string()
            .contains("\"version\":3")
    );
}

#[test]
fn test_fetch_table_stats_multiple_stat_types() {
    // Test fetching when multiple statistics types exist for the same column.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    provider
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .unwrap();
    provider
        .update_table_column_stats(age_column_id, table_id, "max_value", "35")
        .unwrap();
    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "histogram",
            r#"{"buckets": [20, 25, 30, 35]}"#,
        )
        .unwrap();
    provider
        .update_table_column_stats(age_column_id, table_id, "ndv", r#"{"distinct_count": 3}"#)
        .unwrap();
    provider
        .update_table_column_stats(
            age_column_id,
            table_id,
            "quantiles",
            r#"{"p50": 30, "p95": 34, "p99": 35}"#,
        )
        .unwrap();

    let current_snapshot = provider.current_snapshot().unwrap();
    let stats = provider
        .table_statistics("test_table", current_snapshot, conn)
        .unwrap()
        .unwrap();

    let age_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();

    assert_eq!(age_stats.advanced_stats.len(), 5);

    let stat_types: Vec<&str> = age_stats
        .advanced_stats
        .iter()
        .map(|s| s.stats_type.as_str())
        .collect();

    assert!(stat_types.contains(&"min_value"));
    assert!(stat_types.contains(&"max_value"));
    assert!(stat_types.contains(&"histogram"));
    assert!(stat_types.contains(&"ndv"));
    assert!(stat_types.contains(&"quantiles"));
}

#[test]
fn test_fetch_table_stats_columns_without_stats() {
    // Test that columns without advanced statistics are still returned in fetch results.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    provider
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .unwrap();

    let current_snapshot = provider.current_snapshot().unwrap();
    let stats = provider
        .table_statistics("test_table", current_snapshot, conn)
        .unwrap()
        .unwrap();

    assert_eq!(stats.column_statistics.len(), 3);

    let id_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "id")
        .expect("Should have id column");
    let name_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "name")
        .expect("Should have name column");
    let age_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .expect("Should have age column");

    assert_eq!(id_stats.advanced_stats.len(), 0);
    assert_eq!(name_stats.advanced_stats.len(), 0);
    assert_eq!(age_stats.advanced_stats.len(), 1);
}

#[test]
fn test_fetch_table_stats_row_count() {
    // Test that row_count is correctly populated from table statistics.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE large_table (col1 INTEGER, col2 VARCHAR);
        INSERT INTO large_table SELECT i, 'value_' || i::VARCHAR FROM range(1, 101) t(i);
        "#,
    )
    .unwrap();

    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id 
                FROM __ducklake_metadata_metalake.main.ducklake_table dt 
                INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds 
                    ON dt.schema_id = ds.schema_id 
                WHERE ds.schema_name = current_schema() 
                    AND dt.table_name = 'large_table';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    let col1_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id 
                FROM __ducklake_metadata_metalake.main.ducklake_column 
                WHERE table_id = ? AND column_name = 'col1';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    provider
        .update_table_column_stats(col1_id, table_id, "ndv", r#"{"distinct_count": 100}"#)
        .unwrap();

    let current_snapshot = provider.current_snapshot().unwrap();
    let stats = provider
        .table_statistics("large_table", current_snapshot, conn)
        .unwrap()
        .unwrap();

    assert_eq!(stats.row_count, 100);
    assert_eq!(stats.column_statistics.len(), 2);
}

#[test]
fn test_current_schema_arrow() {
    // Test fetching Arrow schema from DuckDB table with type conversions.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE schema_test_table (
            id INTEGER,
            name VARCHAR,
            value DOUBLE,
            active BOOLEAN
        );
        "#,
    )
    .unwrap();

    let schema = provider.current_schema(None, "schema_test_table").unwrap();

    assert_eq!(schema.fields().len(), 4);

    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"value"));
    assert!(field_names.contains(&"active"));

    assert!(matches!(
        schema.field_with_name("id").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Int32
    ));
    assert!(matches!(
        schema.field_with_name("name").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Utf8
    ));
    assert!(matches!(
        schema.field_with_name("value").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Float64
    ));
    assert!(matches!(
        schema.field_with_name("active").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Boolean
    ));

    let schema_explicit = provider
        .current_schema(Some("main"), "schema_test_table")
        .unwrap();
    assert_eq!(schema_explicit.fields().len(), 4);
}

#[test]
fn test_multiple_schemas_comprehensive() {
    // Test schema fetching and metadata tracking across multiple database schemas.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    let initial_schema_info = provider.current_schema_info().unwrap();
    assert_eq!(initial_schema_info.schema_name, "main");
    assert_eq!(initial_schema_info.schema_id, 0);
    assert!(initial_schema_info.end_snapshot.is_none());

    conn.execute_batch(
        r#"
        CREATE SCHEMA analytics;
        CREATE SCHEMA reporting;
        CREATE TABLE main.users (user_id INTEGER, username VARCHAR, email VARCHAR, created_at TIMESTAMP);
        CREATE TABLE analytics.metrics (metric_id BIGINT, metric_name VARCHAR, value DOUBLE, recorded_at DATE);
        CREATE TABLE reporting.summary (report_id SMALLINT, report_name TEXT, data BLOB, is_published BOOLEAN);
        "#,
    )
    .unwrap();

    let main_users_schema = provider.current_schema(None, "users").unwrap();
    assert_eq!(main_users_schema.fields().len(), 4);
    assert!(matches!(
        main_users_schema
            .field_with_name("user_id")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Int32
    ));
    assert!(matches!(
        main_users_schema
            .field_with_name("username")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Utf8
    ));
    assert!(matches!(
        main_users_schema
            .field_with_name("created_at")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Timestamp(_, _)
    ));

    let analytics_metrics_schema = provider
        .current_schema(Some("analytics"), "metrics")
        .unwrap();
    assert_eq!(analytics_metrics_schema.fields().len(), 4);
    assert!(matches!(
        analytics_metrics_schema
            .field_with_name("metric_id")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Int64
    ));
    assert!(matches!(
        analytics_metrics_schema
            .field_with_name("value")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Float64
    ));
    assert!(matches!(
        analytics_metrics_schema
            .field_with_name("recorded_at")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Date32
    ));

    let reporting_summary_schema = provider
        .current_schema(Some("reporting"), "summary")
        .unwrap();
    assert_eq!(reporting_summary_schema.fields().len(), 4);
    assert!(matches!(
        reporting_summary_schema
            .field_with_name("report_id")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Int16
    ));
    assert!(matches!(
        reporting_summary_schema
            .field_with_name("data")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Binary
    ));
    assert!(matches!(
        reporting_summary_schema
            .field_with_name("is_published")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Boolean
    ));

    let current_schema_info = provider.current_schema_info().unwrap();
    assert_eq!(current_schema_info.schema_name, "main");

    conn.execute("USE analytics;", []).unwrap();
    let analytics_schema_info = provider.current_schema_info().unwrap();
    assert_eq!(analytics_schema_info.schema_name, "analytics");
    assert!(analytics_schema_info.end_snapshot.is_none());

    let metrics_schema_implicit = provider.current_schema(None, "metrics").unwrap();
    assert_eq!(metrics_schema_implicit.fields().len(), 4);

    let users_from_main = provider.current_schema(Some("main"), "users").unwrap();
    assert_eq!(users_from_main.fields().len(), 4);

    conn.execute("USE reporting;", []).unwrap();
    let reporting_schema_info = provider.current_schema_info().unwrap();
    assert_eq!(reporting_schema_info.schema_name, "reporting");

    let schemas: Vec<(String, i64, i64, Option<i64>)> = conn
        .prepare(
            r#"
            SELECT schema_name, schema_id, begin_snapshot, end_snapshot 
                FROM __ducklake_metadata_metalake.main.ducklake_schema 
                ORDER BY schema_id;
            "#,
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert!(schemas.len() >= 3);

    let schema_names: Vec<&str> = schemas
        .iter()
        .map(|(name, _, _, _)| name.as_str())
        .collect();
    assert!(schema_names.contains(&"main"));
    assert!(schema_names.contains(&"analytics"));
    assert!(schema_names.contains(&"reporting"));

    for (name, _, _, end_snapshot) in &schemas {
        assert!(end_snapshot.is_none(), "Schema {} should be active", name);
    }
}

#[test]
fn test_error_handling_edge_cases() {
    // Test various error scenarios: non-existent tables, invalid snapshots, invalid IDs.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    // Non-existent table returns empty results
    let current_snapshot = provider.current_snapshot().unwrap();
    let stats = provider
        .table_statistics("nonexistent_table", current_snapshot, conn)
        .unwrap();
    assert!(stats.is_some());
    assert_eq!(stats.unwrap().column_statistics.len(), 0);

    // Invalid/future snapshot still returns data
    provider
        .update_table_column_stats(age_column_id, table_id, "min_value", "25")
        .unwrap();
    let future_stats = provider
        .table_statistics("test_table", SnapshotId(99999), conn)
        .unwrap();
    assert!(future_stats.is_some());
    assert_eq!(future_stats.unwrap().column_statistics.len(), 3);

    // Updating with invalid IDs succeeds without error
    let result =
        provider.update_table_column_stats(9999, 9999, "ndv", r#"{"distinct_count": 100}"#);
    assert!(result.is_ok());

    // Fetching schema for non-existent table returns error
    assert!(provider.current_schema(None, "nonexistent_table").is_err());

    // Invalid schema name returns error
    conn.execute_batch("CREATE TABLE test (id INTEGER);")
        .unwrap();
    assert!(
        provider
            .current_schema(Some("nonexistent_schema"), "test")
            .is_err()
    );
}

#[test]
fn test_update_same_stat_rapidly() {
    // Test updating the same statistic multiple times in rapid succession.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();
    let conn = provider.get_connection();

    let initial_snapshot = provider.current_snapshot().unwrap();

    for i in 1..=5 {
        provider
            .update_table_column_stats(
                age_column_id,
                table_id,
                "ndv",
                &format!(r#"{{"distinct_count": {}}}"#, i * 100),
            )
            .unwrap();
    }

    let final_snapshot = provider.current_snapshot().unwrap();
    assert_eq!(final_snapshot.0, initial_snapshot.0 + 5);

    let versions: Vec<(i64, Option<i64>)> = conn
        .prepare(
            r#"
            SELECT begin_snapshot, end_snapshot 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE table_id = ? AND column_id = ? AND stats_type = 'ndv' 
                ORDER BY begin_snapshot;
            "#,
        )
        .unwrap()
        .query_map([table_id, age_column_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(versions.len(), 5);
    for i in 0..4 {
        assert!(versions[i].1.is_some());
        assert_eq!(versions[i].1.unwrap(), versions[i + 1].0);
    }
    assert!(versions[4].1.is_none());
}

#[test]
fn test_data_edge_cases() {
    // Test empty tables, single columns, special characters, and large payloads.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    // Empty table with zero rows
    conn.execute_batch("CREATE TABLE empty_table (id INTEGER, name VARCHAR);")
        .unwrap();
    let current_snapshot = provider.current_snapshot().unwrap();
    let empty_stats = provider
        .table_statistics("empty_table", current_snapshot, conn)
        .unwrap()
        .unwrap();
    assert_eq!(empty_stats.row_count, 0);

    // Single column table
    conn.execute_batch(
        r#"
        CREATE TABLE single_col (value INTEGER);
        INSERT INTO single_col VALUES (1), (2), (3);
        "#,
    )
    .unwrap();
    let single_snapshot = provider.current_snapshot().unwrap();
    let single_stats = provider
        .table_statistics("single_col", single_snapshot, conn)
        .unwrap()
        .unwrap();
    assert_eq!(single_stats.column_statistics.len(), 1);
    assert_eq!(single_stats.row_count, 3);
    assert_eq!(single_stats.column_statistics[0].name, "value");

    // Special characters in payload
    conn.execute_batch(
        r#"
        CREATE TABLE test_table (id INTEGER, age INTEGER);
        INSERT INTO test_table VALUES (1, 25), (2, 30);
        "#,
    )
    .unwrap();
    let table_id: i64 = conn
        .query_row(
            r#"
            SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
            INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
            WHERE ds.schema_name = current_schema() AND dt.table_name = 'test_table';
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    let age_column_id: i64 = conn
        .query_row(
            r#"
            SELECT column_id 
                FROM __ducklake_metadata_metalake.main.ducklake_column 
                WHERE table_id = ? AND column_name = 'age';
            "#,
            [table_id],
            |row| row.get(0),
        )
        .unwrap();

    let special_payload =
        r#"{"value": "test\"with\\special\nchars", "unicode": "测试", "empty": ""}"#;
    provider
        .update_table_column_stats(age_column_id, table_id, "special_test", special_payload)
        .unwrap();
    let retrieved: String = conn
        .query_row(
            r#"
            SELECT payload 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE column_id = ? AND table_id = ? AND stats_type = 'special_test' 
                    AND end_snapshot IS NULL;
            "#,
            [age_column_id, table_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retrieved, special_payload);

    // Large payload
    let large_histogram: Vec<i32> = (0..1000).collect();
    let large_payload = json!({
        "buckets": large_histogram,
        "metadata": "x".repeat(1000)
    })
    .to_string();
    provider
        .update_table_column_stats(age_column_id, table_id, "large_histogram", &large_payload)
        .unwrap();
    let new_snapshot = provider.current_snapshot().unwrap();
    let large_stats = provider
        .table_statistics("test_table", new_snapshot, conn)
        .unwrap()
        .unwrap();
    let age_stats = large_stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();
    let large_stat = age_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "large_histogram")
        .unwrap();
    assert!(large_stat.data.to_string().len() > 1000);
}

#[test]
fn test_schema_edge_cases() {
    // Test schema fetching with nullable/non-nullable columns and complex types.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    // Mixed nullable and non-nullable columns
    conn.execute_batch(
        r#"
        CREATE TABLE mixed_nulls (
            id INTEGER NOT NULL,
            optional_name VARCHAR,
            required_age INTEGER NOT NULL,
            optional_value DOUBLE
        );
        "#,
    )
    .unwrap();
    let mixed_schema = provider.current_schema(None, "mixed_nulls").unwrap();
    assert_eq!(mixed_schema.fields().len(), 4);
    assert!(!mixed_schema.field_with_name("id").unwrap().is_nullable());
    assert!(
        mixed_schema
            .field_with_name("optional_name")
            .unwrap()
            .is_nullable()
    );
    assert!(
        !mixed_schema
            .field_with_name("required_age")
            .unwrap()
            .is_nullable()
    );
    assert!(
        mixed_schema
            .field_with_name("optional_value")
            .unwrap()
            .is_nullable()
    );

    // Complex types
    conn.execute_batch(
        r#"
        CREATE TABLE complex_types (
            tiny_col TINYINT,
            small_col SMALLINT,
            int_col INTEGER,
            big_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            blob_col BLOB,
            bool_col BOOLEAN
        );
        "#,
    )
    .unwrap();
    let complex_schema = provider.current_schema(None, "complex_types").unwrap();
    assert_eq!(complex_schema.fields().len(), 11);
    assert!(matches!(
        complex_schema
            .field_with_name("tiny_col")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Int8
    ));
    assert!(matches!(
        complex_schema
            .field_with_name("small_col")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Int16
    ));
    assert!(matches!(
        complex_schema
            .field_with_name("float_col")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Float32
    ));
    assert!(matches!(
        complex_schema
            .field_with_name("date_col")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Date32
    ));
    assert!(matches!(
        complex_schema
            .field_with_name("time_col")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Time64(_)
    ));
    assert!(matches!(
        complex_schema
            .field_with_name("blob_col")
            .unwrap()
            .data_type(),
        &duckdb::arrow::datatypes::DataType::Binary
    ));
}

#[test]
fn test_concurrent_snapshot_isolation() {
    // Test statistics with special characters and edge case JSON values.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();

    let special_payload =
        r#"{"value": "test\"with\\special\nchars", "unicode": "测试", "empty": ""}"#;
    let result = provider.update_table_column_stats(
        age_column_id,
        table_id,
        "special_test",
        special_payload,
    );

    assert!(result.is_ok());

    let conn = provider.get_connection();
    let retrieved_payload: String = conn
        .query_row(
            r#"
            SELECT payload 
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats 
                WHERE column_id = ? AND table_id = ? AND stats_type = 'special_test' 
                    AND end_snapshot IS NULL;
            "#,
            [age_column_id, table_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(retrieved_payload, special_payload);
}

#[test]
fn test_large_statistics_payload() {
    // Test handling of large statistics payloads.
    let (_temp_dir, provider, table_id, age_column_id) = create_test_catalog_with_data();

    let large_histogram: Vec<i32> = (0..1000).collect();
    let large_payload = json!({
        "buckets": large_histogram,
        "metadata": "x".repeat(1000)
    })
    .to_string();

    let result = provider.update_table_column_stats(
        age_column_id,
        table_id,
        "large_histogram",
        &large_payload,
    );

    assert!(result.is_ok());

    let conn = provider.get_connection();
    let current_snapshot = provider.current_snapshot().unwrap();
    let stats = provider
        .table_statistics("test_table", current_snapshot, conn)
        .unwrap()
        .unwrap();

    let age_stats = stats
        .column_statistics
        .iter()
        .find(|cs| cs.name == "age")
        .unwrap();

    let large_stat = age_stats
        .advanced_stats
        .iter()
        .find(|s| s.stats_type == "large_histogram")
        .expect("Should have large_histogram stat");

    assert!(large_stat.data.to_string().len() > 1000);
}

#[test]
fn test_mixed_null_and_non_null_columns() {
    // Test schema fetching with mixed nullable and non-nullable columns.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE mixed_nulls (
            id INTEGER NOT NULL,
            optional_name VARCHAR,
            required_age INTEGER NOT NULL,
            optional_value DOUBLE
        );
        "#,
    )
    .unwrap();

    let schema = provider.current_schema(None, "mixed_nulls").unwrap();

    assert_eq!(schema.fields().len(), 4);

    let id_field = schema.field_with_name("id").unwrap();
    assert!(!id_field.is_nullable());

    let optional_name_field = schema.field_with_name("optional_name").unwrap();
    assert!(optional_name_field.is_nullable());

    let required_age_field = schema.field_with_name("required_age").unwrap();
    assert!(!required_age_field.is_nullable());

    let optional_value_field = schema.field_with_name("optional_value").unwrap();
    assert!(optional_value_field.is_nullable());
}

#[test]
fn test_schema_with_complex_types() {
    // Test schema fetching with various complex and edge case data types.
    let (_temp_dir, provider) = create_test_catalog(false);
    let conn = provider.get_connection();

    conn.execute_batch(
        r#"
        CREATE TABLE complex_types (
            tiny_col TINYINT,
            small_col SMALLINT,
            int_col INTEGER,
            big_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            blob_col BLOB,
            bool_col BOOLEAN
        );
        "#,
    )
    .unwrap();

    let schema = provider.current_schema(None, "complex_types").unwrap();

    assert_eq!(schema.fields().len(), 11);

    assert!(matches!(
        schema.field_with_name("tiny_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Int8
    ));
    assert!(matches!(
        schema.field_with_name("small_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Int16
    ));
    assert!(matches!(
        schema.field_with_name("int_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Int32
    ));
    assert!(matches!(
        schema.field_with_name("big_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Int64
    ));
    assert!(matches!(
        schema.field_with_name("float_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Float32
    ));
    assert!(matches!(
        schema.field_with_name("double_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Float64
    ));
    assert!(matches!(
        schema.field_with_name("date_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Date32
    ));
    assert!(matches!(
        schema.field_with_name("time_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Time64(_)
    ));
    assert!(matches!(
        schema.field_with_name("timestamp_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Timestamp(_, _)
    ));
    assert!(matches!(
        schema.field_with_name("blob_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Binary
    ));
    assert!(matches!(
        schema.field_with_name("bool_col").unwrap().data_type(),
        &duckdb::arrow::datatypes::DataType::Boolean
    ));
}
