//! Unified statistics API tests for both external and internal tables.

use optd_catalog::{
    AdvanceColumnStatistics, Catalog, ColumnStatistics, DuckLakeCatalog, RegisterTableRequest,
    TableStatistics,
};
use serde_json::json;
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_catalog() -> (TempDir, DuckLakeCatalog) {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    (temp_dir, catalog)
}

/// Creates ColumnStatistics for external tables.
fn create_column_stats(
    column_name: &str,
    stats_type: &str,
    data: serde_json::Value,
) -> ColumnStatistics {
    ColumnStatistics {
        column_id: 0,
        column_type: String::new(),
        name: column_name.to_string(),
        advanced_stats: vec![AdvanceColumnStatistics {
            stats_type: stats_type.to_string(),
            data,
        }],
        min_value: None,
        max_value: None,
        null_count: None,
        distinct_count: None,
    }
}

#[test]
fn test_set_and_get_table_statistics_row_count_only() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create an external table
    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: None,
        location: "/tmp/test.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set statistics (row count only)
    let stats = TableStatistics {
        row_count: 1000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "test_table", stats)
        .unwrap();

    // Get and verify statistics
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved_stats = catalog.table_statistics("test_table", snapshot).unwrap();
    assert!(retrieved_stats.is_some());
    let retrieved_stats = retrieved_stats.unwrap();
    assert_eq!(retrieved_stats.row_count, 1000);
    assert!(retrieved_stats.column_statistics.is_empty());
}

#[test]
fn test_set_statistics_with_column_stats() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create an external table
    let request = RegisterTableRequest {
        table_name: "users".to_string(),
        schema_name: None,
        location: "/tmp/users.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set statistics with column stats
    let column_stats = vec![
        create_column_stats(
            "id",
            "basic",
            json!({
                "min_value": "1",
                "max_value": "1000",
                "null_count": 0,
                "distinct_count": 1000
            }),
        ),
        create_column_stats(
            "age",
            "basic",
            json!({
                "min_value": "18",
                "max_value": "80",
                "null_count": 5,
                "distinct_count": 50
            }),
        ),
    ];

    let stats = TableStatistics {
        row_count: 1000,
        column_statistics: column_stats,
        size_bytes: None,
    };
    catalog.set_table_statistics(None, "users", stats).unwrap();

    // Get and verify statistics
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved_stats = catalog
        .table_statistics("users", snapshot)
        .unwrap()
        .unwrap();

    assert_eq!(retrieved_stats.row_count, 1000);
    assert_eq!(retrieved_stats.column_statistics.len(), 2);

    // Find id column stats
    let id_stats = retrieved_stats
        .column_statistics
        .iter()
        .find(|s| s.name == "id")
        .unwrap();
    assert_eq!(id_stats.name, "id");
    assert_eq!(id_stats.column_id, 0); // External table sentinel
    assert_eq!(id_stats.advanced_stats.len(), 1);
    assert_eq!(id_stats.advanced_stats[0].stats_type, "basic");
    assert_eq!(id_stats.advanced_stats[0].data["min_value"], "1");
    assert_eq!(id_stats.advanced_stats[0].data["max_value"], "1000");
    assert_eq!(id_stats.advanced_stats[0].data["null_count"], 0);
    assert_eq!(id_stats.advanced_stats[0].data["distinct_count"], 1000);

    // Find age column stats
    let age_stats = retrieved_stats
        .column_statistics
        .iter()
        .find(|s| s.name == "age")
        .unwrap();
    assert_eq!(age_stats.name, "age");
    assert_eq!(age_stats.column_id, 0);
    assert_eq!(age_stats.advanced_stats[0].stats_type, "basic");
    assert_eq!(age_stats.advanced_stats[0].data["min_value"], "18");
    assert_eq!(age_stats.advanced_stats[0].data["max_value"], "80");
    assert_eq!(age_stats.advanced_stats[0].data["null_count"], 5);
    assert_eq!(age_stats.advanced_stats[0].data["distinct_count"], 50);
}

#[test]
fn test_flexible_stats_types() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table
    let request = RegisterTableRequest {
        table_name: "products".to_string(),
        schema_name: None,
        location: "/tmp/products.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set statistics with different stats types
    let column_stats = vec![
        create_column_stats(
            "price",
            "basic",
            json!({
                "min_value": "0.99",
                "max_value": "999.99",
                "null_count": 0,
                "distinct_count": 500
            }),
        ),
        create_column_stats(
            "age",
            "histogram",
            json!({
                "buckets": [
                    {"lower": 0, "upper": 20, "count": 1500},
                    {"lower": 20, "upper": 40, "count": 3000},
                    {"lower": 40, "upper": 60, "count": 2500},
                    {"lower": 60, "upper": 100, "count": 1000}
                ]
            }),
        ),
        create_column_stats(
            "user_id",
            "hyperloglog",
            json!({
                "register_values": "base64encodeddata==",
                "estimated_cardinality": 8543
            }),
        ),
    ];

    let stats = TableStatistics {
        row_count: 8000,
        column_statistics: column_stats,
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "products", stats)
        .unwrap();

    // Get and verify all stat types are preserved
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved_stats = catalog
        .table_statistics("products", snapshot)
        .unwrap()
        .unwrap();

    assert_eq!(retrieved_stats.row_count, 8000);
    assert_eq!(retrieved_stats.column_statistics.len(), 3);

    // Verify basic stats
    let price_stats = retrieved_stats
        .column_statistics
        .iter()
        .find(|s| s.name == "price")
        .unwrap();
    assert_eq!(price_stats.advanced_stats[0].stats_type, "basic");

    // Verify histogram
    let age_stats = retrieved_stats
        .column_statistics
        .iter()
        .find(|s| s.name == "age")
        .unwrap();
    assert_eq!(age_stats.advanced_stats[0].stats_type, "histogram");
    let buckets = &age_stats.advanced_stats[0].data["buckets"];
    assert_eq!(buckets.as_array().unwrap().len(), 4);

    // Verify hyperloglog
    let user_stats = retrieved_stats
        .column_statistics
        .iter()
        .find(|s| s.name == "user_id")
        .unwrap();
    assert_eq!(user_stats.advanced_stats[0].stats_type, "hyperloglog");
    assert_eq!(
        user_stats.advanced_stats[0].data["estimated_cardinality"],
        8543
    );
}

#[test]
fn test_update_statistics() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table
    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: None,
        location: "/tmp/test.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set initial statistics
    let stats1 = TableStatistics {
        row_count: 1000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "test_table", stats1)
        .unwrap();

    // Verify initial
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved1 = catalog
        .table_statistics("test_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved1.row_count, 1000);

    // Update statistics
    let stats2 = TableStatistics {
        row_count: 2000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "test_table", stats2)
        .unwrap();

    // Verify update
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved2 = catalog
        .table_statistics("test_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved2.row_count, 2000);
}

#[test]
fn test_nonexistent_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let snapshot = catalog.current_snapshot().unwrap();
    let result = catalog.table_statistics("nonexistent", snapshot);
    assert!(result.unwrap().is_none());
}
