//! Tests for statistics with internal DuckDB tables.

use optd_catalog::{
    AdvanceColumnStatistics, Catalog, ColumnStatistics, DuckLakeCatalog, TableStatistics,
};
use serde_json::json;
use tempfile::TempDir;

fn create_test_catalog() -> (TempDir, DuckLakeCatalog) {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    (temp_dir, catalog)
}

#[test]
fn test_internal_table_statistics_basic() {
    let (_temp_dir, mut catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    // Create an internal DuckDB table
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
         INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);",
    )
    .unwrap();

    // Set statistics for internal table
    let stats = TableStatistics {
        row_count: 2,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog.set_table_statistics(None, "users", stats).unwrap();

    // Get statistics
    let retrieved = catalog.get_table_statistics_manual(None, "users").unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().row_count, 2);
}

#[test]
fn test_internal_table_with_column_statistics() {
    let (_temp_dir, mut catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    // Create internal table
    conn.execute_batch(
        "CREATE TABLE products (id INTEGER, price DECIMAL(10,2), category VARCHAR);",
    )
    .unwrap();

    // Get column IDs from ducklake_column
    let id_column_id: i64 = conn
        .query_row(
            "SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column dc
             INNER JOIN __ducklake_metadata_metalake.main.ducklake_table dt ON dc.table_id = dt.table_id
             INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
             WHERE ds.schema_name = current_schema() AND dt.table_name = 'products' AND dc.column_name = 'id'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    let price_column_id: i64 = conn
        .query_row(
            "SELECT column_id FROM __ducklake_metadata_metalake.main.ducklake_column dc
             INNER JOIN __ducklake_metadata_metalake.main.ducklake_table dt ON dc.table_id = dt.table_id
             INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
             WHERE ds.schema_name = current_schema() AND dt.table_name = 'products' AND dc.column_name = 'price'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    // Set statistics with column stats (using actual column_ids)
    let column_stats = vec![
        ColumnStatistics {
            column_id: id_column_id,
            column_type: "INTEGER".to_string(),
            name: "id".to_string(), // Human-readable name
            advanced_stats: vec![AdvanceColumnStatistics {
                stats_type: "basic".to_string(),
                data: json!({
                    "min_value": "1",
                    "max_value": "1000",
                    "distinct_count": 1000
                }),
            }],
            min_value: None,
            max_value: None,
            null_count: None,
            distinct_count: None,
        },
        ColumnStatistics {
            column_id: price_column_id,
            column_type: "DECIMAL".to_string(),
            name: "price".to_string(),
            advanced_stats: vec![AdvanceColumnStatistics {
                stats_type: "basic".to_string(),
                data: json!({
                    "min_value": "9.99",
                    "max_value": "999.99",
                    "null_count": 0
                }),
            }],
            min_value: None,
            max_value: None,
            null_count: None,
            distinct_count: None,
        },
    ];

    let stats = TableStatistics {
        row_count: 1000,
        column_statistics: column_stats,
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "products", stats)
        .unwrap();

    // Get and verify statistics
    let retrieved = catalog
        .get_table_statistics_manual(None, "products")
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.row_count, 1000);
    assert_eq!(retrieved.column_statistics.len(), 2);

    // Verify column names are properly looked up (not "1", "2")
    let id_stats = retrieved
        .column_statistics
        .iter()
        .find(|c| c.column_id == id_column_id)
        .unwrap();
    assert_eq!(
        id_stats.name, "id",
        "Column name should be 'id', not column_id"
    );
    assert_eq!(id_stats.advanced_stats.len(), 1);
    assert_eq!(id_stats.advanced_stats[0].stats_type, "basic");

    let price_stats = retrieved
        .column_statistics
        .iter()
        .find(|c| c.column_id == price_column_id)
        .unwrap();
    assert_eq!(
        price_stats.name, "price",
        "Column name should be 'price', not column_id"
    );
}

#[test]
fn test_internal_and_external_tables_separate_catalogs() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create internal table
    {
        let conn = catalog.get_connection();
        conn.execute_batch("CREATE TABLE internal_users (id INTEGER, name VARCHAR);")
            .unwrap();
    }

    // Get table_id for internal table (should be < 1000000)
    let internal_table_id: i64 = {
        let conn = catalog.get_connection();
        conn.query_row(
            "SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table dt
             INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
             WHERE ds.schema_name = current_schema() AND dt.table_name = 'internal_users'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    // Register external table
    use optd_catalog::RegisterTableRequest;
    use std::collections::HashMap;
    let request = RegisterTableRequest {
        table_name: "external_logs".to_string(),
        schema_name: None,
        location: "/tmp/logs.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Get table_id for external table (should be >= 1000000)
    let external_table_id: i64 = {
        let conn = catalog.get_connection();
        conn.query_row(
            "SELECT table_id FROM __ducklake_metadata_metalake.main.optd_external_table dt
             INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
             WHERE ds.schema_name = current_schema() AND dt.table_name = 'external_logs' AND dt.end_snapshot IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    // Verify separate ID ranges
    // Internal tables: positive IDs (1, 2, 3, ...)
    // External tables: negative IDs (-1, -2, -3, ...)
    assert!(
        internal_table_id > 0,
        "Internal table_id should be positive, got {}",
        internal_table_id
    );
    assert!(
        external_table_id < 0,
        "External table_id should be negative, got {}",
        external_table_id
    );

    // Set stats for internal table
    let internal_stats = TableStatistics {
        row_count: 100,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "internal_users", internal_stats)
        .unwrap();

    // Set stats for external table
    let external_stats = TableStatistics {
        row_count: 500,
        column_statistics: vec![ColumnStatistics {
            column_id: 0, // Sentinel for external
            column_type: String::new(),
            name: "timestamp".to_string(),
            advanced_stats: vec![AdvanceColumnStatistics {
                stats_type: "basic".to_string(),
                data: json!({"min_value": "2024-01-01", "max_value": "2024-12-31"}),
            }],
            min_value: None,
            max_value: None,
            null_count: None,
            distinct_count: None,
        }],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "external_logs", external_stats)
        .unwrap();

    // Verify both work independently (no collision!)
    let internal = catalog
        .get_table_statistics_manual(None, "internal_users")
        .unwrap();
    let external = catalog
        .get_table_statistics_manual(None, "external_logs")
        .unwrap();

    assert!(internal.is_some(), "internal_users statistics should exist");
    assert!(external.is_some(), "external_logs statistics should exist");

    let internal = internal.unwrap();
    let external = external.unwrap();

    assert_eq!(
        internal.row_count, 100,
        "internal_users should have 100 rows (not overwritten!)"
    );
    assert_eq!(
        external.row_count, 500,
        "external_logs should have 500 rows"
    );
    assert_eq!(external.column_statistics.len(), 1);
    assert_eq!(external.column_statistics[0].name, "timestamp");
}

#[test]
fn test_internal_table_update_statistics() {
    let (_temp_dir, mut catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    // Create internal table
    conn.execute_batch("CREATE TABLE events (id INTEGER, event_type VARCHAR);")
        .unwrap();

    // Set initial statistics
    let stats1 = TableStatistics {
        row_count: 1000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "events", stats1)
        .unwrap();

    // Update statistics
    let stats2 = TableStatistics {
        row_count: 2000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "events", stats2)
        .unwrap();

    // Verify updated value
    let retrieved = catalog
        .get_table_statistics_manual(None, "events")
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.row_count, 2000);
}
