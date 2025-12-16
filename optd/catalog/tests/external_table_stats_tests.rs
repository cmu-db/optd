//! External table statistics tests.

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
fn col_stats(column_name: &str, stats_type: &str, data: serde_json::Value) -> ColumnStatistics {
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
fn test_set_and_get_external_table_statistics() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create an external table first
    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: None,
        location: "/tmp/test.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set statistics
    let stats = TableStatistics {
        row_count: 1000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "test_table", stats)
        .unwrap();

    // Get statistics
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
        col_stats(
            "id",
            "basic",
            json!({
                "min_value": "1",
                "max_value": "1000",
                "null_count": 0,
                "distinct_count": 1000
            }),
        ),
        col_stats(
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
    assert_eq!(age_stats.advanced_stats[0].stats_type, "basic");
    assert_eq!(age_stats.advanced_stats[0].data["min_value"], "18");
    assert_eq!(age_stats.advanced_stats[0].data["max_value"], "80");
    assert_eq!(age_stats.advanced_stats[0].data["null_count"], 5);
    assert_eq!(age_stats.advanced_stats[0].data["distinct_count"], 50);
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
        row_count: 100,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "test_table", stats1)
        .unwrap();

    // Verify initial statistics
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved1 = catalog
        .table_statistics("test_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved1.row_count, 100);

    // Update statistics
    let stats2 = TableStatistics {
        row_count: 200,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "test_table", stats2)
        .unwrap();

    // Verify updated statistics
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved2 = catalog
        .table_statistics("test_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved2.row_count, 200);
}

#[test]
fn test_get_statistics_for_nonexistent_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let snapshot = catalog.current_snapshot().unwrap();
    let result = catalog.table_statistics("nonexistent", snapshot);
    assert!(result.unwrap().is_none());
}

#[test]
fn test_get_statistics_without_setting_them() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table but don't set statistics
    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: None,
        location: "/tmp/test.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    let snapshot = catalog.current_snapshot().unwrap();
    let result = catalog.table_statistics("test_table", snapshot);
    assert!(result.unwrap().is_none());
}

#[test]
fn test_statistics_persist_across_catalog_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // First session: create table and set statistics
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        let request = RegisterTableRequest {
            table_name: "persistent_table".to_string(),
            schema_name: None,
            location: "/tmp/persistent.csv".to_string(),
            file_format: "CSV".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        catalog.register_external_table(request).unwrap();

        let stats = TableStatistics {
            row_count: 5000,
            column_statistics: vec![],
            size_bytes: None,
        };
        catalog
            .set_table_statistics(None, "persistent_table", stats)
            .unwrap();
    } // catalog dropped here

    // Second session: reconnect and verify statistics persist
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        let snapshot = catalog.current_snapshot().unwrap();
        let retrieved = catalog
            .table_statistics("persistent_table", snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.row_count, 5000);
    }
}

#[test]
fn test_update_statistics_with_column_stats() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table
    let request = RegisterTableRequest {
        table_name: "evolving_table".to_string(),
        schema_name: None,
        location: "/tmp/evolving.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set initial statistics with one column
    let initial_stats = TableStatistics {
        row_count: 1000,
        column_statistics: vec![col_stats(
            "id",
            "basic",
            json!({
                "min_value": "1",
                "max_value": "1000",
                "null_count": 0,
                "distinct_count": 1000
            }),
        )],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "evolving_table", initial_stats)
        .unwrap();

    // Verify initial
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved_initial = catalog
        .table_statistics("evolving_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved_initial.row_count, 1000);
    assert_eq!(retrieved_initial.column_statistics.len(), 1);

    // Update with different columns
    let updated_stats = TableStatistics {
        row_count: 2000,
        column_statistics: vec![
            col_stats(
                "id",
                "basic",
                json!({
                    "min_value": "1",
                    "max_value": "2000",
                    "null_count": 0,
                    "distinct_count": 2000
                }),
            ),
            col_stats(
                "name",
                "basic",
                json!({
                    "min_value": "Alice",
                    "max_value": "Zoe",
                    "null_count": 10,
                    "distinct_count": 1500
                }),
            ),
        ],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "evolving_table", updated_stats)
        .unwrap();

    // Verify update
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved_updated = catalog
        .table_statistics("evolving_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved_updated.row_count, 2000);
    assert_eq!(retrieved_updated.column_statistics.len(), 2);

    // Verify old stats are gone, new stats are present
    let id_stats = retrieved_updated
        .column_statistics
        .iter()
        .find(|s| s.name == "id")
        .unwrap();
    assert_eq!(id_stats.advanced_stats[0].data["max_value"], "2000");

    let name_stats = retrieved_updated
        .column_statistics
        .iter()
        .find(|s| s.name == "name")
        .unwrap();
    assert_eq!(name_stats.advanced_stats[0].data["distinct_count"], 1500);
}

#[test]
fn test_partial_column_statistics() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table
    let request = RegisterTableRequest {
        table_name: "partial_stats_table".to_string(),
        schema_name: None,
        location: "/tmp/partial.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set statistics with different stats_types for different columns
    let stats = TableStatistics {
        row_count: 3000,
        column_statistics: vec![
            col_stats(
                "price",
                "min_max",
                json!({
                    "min_value": "0.99",
                    "max_value": "999.99"
                }),
            ),
            col_stats(
                "quantity",
                "cardinality",
                json!({
                    "distinct_count": 50
                }),
            ),
        ],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "partial_stats_table", stats)
        .unwrap();

    // Verify
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved = catalog
        .table_statistics("partial_stats_table", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.row_count, 3000);
    assert_eq!(retrieved.column_statistics.len(), 2);

    let price_stats = retrieved
        .column_statistics
        .iter()
        .find(|s| s.name == "price")
        .unwrap();
    assert_eq!(price_stats.advanced_stats[0].stats_type, "min_max");
    assert!(
        price_stats.advanced_stats[0]
            .data
            .get("min_value")
            .is_some()
    );

    let quantity_stats = retrieved
        .column_statistics
        .iter()
        .find(|s| s.name == "quantity")
        .unwrap();
    assert_eq!(quantity_stats.advanced_stats[0].stats_type, "cardinality");
    assert!(
        quantity_stats.advanced_stats[0]
            .data
            .get("distinct_count")
            .is_some()
    );
}

#[test]
fn test_statistics_with_empty_column_stats_vec() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table
    let request = RegisterTableRequest {
        table_name: "empty_col_stats".to_string(),
        schema_name: None,
        location: "/tmp/empty.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Set statistics with empty column stats
    let stats = TableStatistics {
        row_count: 100,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "empty_col_stats", stats)
        .unwrap();

    // Verify
    let snapshot = catalog.current_snapshot().unwrap();
    let retrieved = catalog
        .table_statistics("empty_col_stats", snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.row_count, 100);
    assert!(retrieved.column_statistics.is_empty());
}

#[test]
fn test_multiple_updates_create_new_snapshots() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Create table
    let request = RegisterTableRequest {
        table_name: "snapshot_table".to_string(),
        schema_name: None,
        location: "/tmp/snapshot.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    // Get initial snapshot
    let snapshot1 = catalog.current_snapshot().unwrap();

    // First update
    let stats1 = TableStatistics {
        row_count: 1000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "snapshot_table", stats1)
        .unwrap();

    // Snapshot should not have changed yet (stats don't create snapshot on first set)
    let snapshot2 = catalog.current_snapshot().unwrap();
    assert_eq!(snapshot1.0, snapshot2.0);

    // Second update - should create new snapshot
    let stats2 = TableStatistics {
        row_count: 2000,
        column_statistics: vec![],
        size_bytes: None,
    };
    catalog
        .set_table_statistics(None, "snapshot_table", stats2)
        .unwrap();

    let snapshot3 = catalog.current_snapshot().unwrap();
    assert!(
        snapshot3.0 > snapshot2.0,
        "Snapshot should have incremented after update"
    );
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
        col_stats(
            "price",
            "basic",
            json!({
                "min_value": "0.99",
                "max_value": "999.99",
                "null_count": 0,
                "distinct_count": 500
            }),
        ),
        col_stats(
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
        col_stats(
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

/// **CRITICAL INTEGRATION TEST**: Validates the full optimizer statistics workflow.
///
/// This test proves that the optimizer can store and retrieve ANY type of statistics
/// across catalog restarts (sessions). It tests:
///
/// 1. **Multiple Statistics Types**: Basic, Histogram, HyperLogLog, MCV, Bloom Filter, Correlation
/// 2. **Cross-Session Persistence**: Statistics survive catalog drops and reconnections
/// 3. **Update Semantics**: Old statistics are replaced when setting new ones
/// 4. **Complex Payloads**: Nested JSON structures with arrays and objects persist correctly
///
/// This is the definitive proof that the catalog can serve as a persistent statistics
/// store for cost-based query optimization.
#[test]
fn test_advanced_statistics_persist_across_sessions() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // ========== SESSION 1: Create table and set advanced statistics ==========
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        // Register external table
        let request = RegisterTableRequest {
            table_name: "optimizer_stats_table".to_string(),
            schema_name: None,
            location: "/data/analytics.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        catalog.register_external_table(request).unwrap();

        // Set comprehensive statistics with multiple advanced types
        let column_stats = vec![
            // Basic statistics (min/max/null/distinct)
            col_stats(
                "id",
                "basic",
                json!({
                    "min_value": "1",
                    "max_value": "100000",
                    "null_count": 0,
                    "distinct_count": 100000
                }),
            ),
            // Histogram for range queries
            col_stats(
                "age",
                "histogram",
                json!({
                    "buckets": [
                        {"lower": 0, "upper": 18, "count": 5000, "distinct": 18},
                        {"lower": 18, "upper": 35, "count": 35000, "distinct": 17},
                        {"lower": 35, "upper": 50, "count": 30000, "distinct": 15},
                        {"lower": 50, "upper": 65, "count": 20000, "distinct": 15},
                        {"lower": 65, "upper": 100, "count": 10000, "distinct": 35}
                    ],
                    "total_count": 100000
                }),
            ),
            // HyperLogLog sketch for cardinality estimation
            col_stats(
                "user_id",
                "hyperloglog",
                json!({
                    "register_values": "AgEBAwIEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2A=",
                    "estimated_cardinality": 87543,
                    "precision": 14
                }),
            ),
            // Most Common Values (MCV) for skewed columns
            col_stats(
                "country",
                "most_common_values",
                json!({
                    "values": [
                        {"value": "USA", "frequency": 0.45, "count": 45000},
                        {"value": "UK", "frequency": 0.20, "count": 20000},
                        {"value": "Canada", "frequency": 0.15, "count": 15000},
                        {"value": "Germany", "frequency": 0.10, "count": 10000},
                        {"value": "France", "frequency": 0.05, "count": 5000}
                    ],
                    "other_count": 5000,
                    "total_distinct": 25
                }),
            ),
            // Bloom filter for membership testing
            col_stats(
                "email",
                "bloom_filter",
                json!({
                    "bit_array": "base64encodedbloomfilterdata==",
                    "num_bits": 1000000,
                    "num_hash_functions": 7,
                    "expected_fpp": 0.01
                }),
            ),
            // Correlation statistics for multi-column optimization
            col_stats(
                "created_at",
                "correlation",
                json!({
                    "correlated_columns": {
                        "user_id": 0.85,
                        "age": -0.12,
                        "country": 0.03
                    },
                    "temporal_distribution": {
                        "min_timestamp": "2020-01-01T00:00:00Z",
                        "max_timestamp": "2024-12-15T23:59:59Z",
                        "peak_hour": 14,
                        "weekend_ratio": 0.28
                    }
                }),
            ),
        ];

        let stats = TableStatistics {
            row_count: 100000,
            column_statistics: column_stats,
            size_bytes: Some(52428800), // 50 MB
        };

        catalog
            .set_table_statistics(None, "optimizer_stats_table", stats)
            .unwrap();

        // Verify stats are accessible in this session
        let snapshot = catalog.current_snapshot().unwrap();
        let retrieved = catalog
            .table_statistics("optimizer_stats_table", snapshot)
            .unwrap()
            .expect("Stats should be set");
        assert_eq!(retrieved.row_count, 100000);
        assert_eq!(retrieved.column_statistics.len(), 6);
        assert_eq!(retrieved.size_bytes, Some(52428800));
    } // Catalog dropped here - simulates session end

    // ========== SESSION 2: Reconnect and verify all statistics persisted ==========
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        let snapshot = catalog.current_snapshot().unwrap();
        let retrieved = catalog
            .table_statistics("optimizer_stats_table", snapshot)
            .unwrap()
            .expect("Stats should persist across sessions");

        // Verify table-level statistics
        assert_eq!(retrieved.row_count, 100000, "Row count should persist");
        assert_eq!(
            retrieved.size_bytes,
            Some(52428800),
            "File size should persist"
        );
        assert_eq!(
            retrieved.column_statistics.len(),
            6,
            "All 6 column statistics should persist"
        );

        // Verify Basic statistics (id column)
        let id_stats = retrieved
            .column_statistics
            .iter()
            .find(|s| s.name == "id")
            .expect("id column stats should exist");
        assert_eq!(id_stats.advanced_stats[0].stats_type, "basic");
        assert_eq!(id_stats.advanced_stats[0].data["min_value"], "1");
        assert_eq!(id_stats.advanced_stats[0].data["max_value"], "100000");
        assert_eq!(id_stats.advanced_stats[0].data["null_count"], 0);
        assert_eq!(id_stats.advanced_stats[0].data["distinct_count"], 100000);

        // Verify Histogram (age column)
        let age_stats = retrieved
            .column_statistics
            .iter()
            .find(|s| s.name == "age")
            .expect("age column stats should exist");
        assert_eq!(age_stats.advanced_stats[0].stats_type, "histogram");
        let buckets = age_stats.advanced_stats[0].data["buckets"]
            .as_array()
            .expect("histogram should have buckets");
        assert_eq!(buckets.len(), 5, "Histogram should have 5 buckets");
        assert_eq!(buckets[0]["lower"], 0);
        assert_eq!(buckets[0]["upper"], 18);
        assert_eq!(buckets[0]["count"], 5000);
        assert_eq!(age_stats.advanced_stats[0].data["total_count"], 100000);

        // Verify HyperLogLog (user_id column)
        let user_id_stats = retrieved
            .column_statistics
            .iter()
            .find(|s| s.name == "user_id")
            .expect("user_id column stats should exist");
        assert_eq!(user_id_stats.advanced_stats[0].stats_type, "hyperloglog");
        assert_eq!(
            user_id_stats.advanced_stats[0].data["estimated_cardinality"],
            87543
        );
        assert_eq!(user_id_stats.advanced_stats[0].data["precision"], 14);
        assert!(
            user_id_stats.advanced_stats[0].data["register_values"]
                .as_str()
                .unwrap()
                .len()
                > 50,
            "HyperLogLog register values should be preserved"
        );

        // Verify Most Common Values (country column)
        let country_stats = retrieved
            .column_statistics
            .iter()
            .find(|s| s.name == "country")
            .expect("country column stats should exist");
        assert_eq!(
            country_stats.advanced_stats[0].stats_type,
            "most_common_values"
        );
        let mcv_values = country_stats.advanced_stats[0].data["values"]
            .as_array()
            .expect("MCV should have values array");
        assert_eq!(mcv_values.len(), 5, "Should have 5 most common values");
        assert_eq!(mcv_values[0]["value"], "USA");
        assert_eq!(mcv_values[0]["frequency"], 0.45);
        assert_eq!(mcv_values[0]["count"], 45000);
        assert_eq!(country_stats.advanced_stats[0].data["total_distinct"], 25);

        // Verify Bloom Filter (email column)
        let email_stats = retrieved
            .column_statistics
            .iter()
            .find(|s| s.name == "email")
            .expect("email column stats should exist");
        assert_eq!(email_stats.advanced_stats[0].stats_type, "bloom_filter");
        assert_eq!(email_stats.advanced_stats[0].data["num_bits"], 1000000);
        assert_eq!(email_stats.advanced_stats[0].data["num_hash_functions"], 7);
        assert_eq!(email_stats.advanced_stats[0].data["expected_fpp"], 0.01);

        // Verify Correlation statistics (created_at column)
        let created_at_stats = retrieved
            .column_statistics
            .iter()
            .find(|s| s.name == "created_at")
            .expect("created_at column stats should exist");
        assert_eq!(created_at_stats.advanced_stats[0].stats_type, "correlation");
        let corr_cols = &created_at_stats.advanced_stats[0].data["correlated_columns"];
        assert_eq!(corr_cols["user_id"], 0.85);
        assert_eq!(corr_cols["age"], -0.12);
        let temporal = &created_at_stats.advanced_stats[0].data["temporal_distribution"];
        assert_eq!(temporal["peak_hour"], 14);
        assert_eq!(temporal["weekend_ratio"], 0.28);

        println!(
            "✅ INTEGRATION TEST PASSED: All advanced statistics types persist across sessions!"
        );
        println!("   - Basic stats (min/max/null/distinct): ✓");
        println!("   - Histogram (5 buckets): ✓");
        println!("   - HyperLogLog (87543 estimated cardinality): ✓");
        println!("   - Most Common Values (5 entries): ✓");
        println!("   - Bloom Filter (1M bits, 7 hash functions): ✓");
        println!("   - Correlation (3 correlated columns + temporal): ✓");
        println!("\n   This proves the optimizer can store and retrieve ANY statistics type!");
    }

    // ========== SESSION 3: Update statistics and verify updates persist ==========
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        // Simulate optimizer updating statistics after analyzing new data
        let updated_stats = TableStatistics {
            row_count: 150000, // Table grew
            column_statistics: vec![
                col_stats(
                    "id",
                    "basic",
                    json!({
                        "min_value": "1",
                        "max_value": "150000",
                        "null_count": 0,
                        "distinct_count": 150000
                    }),
                ),
                col_stats(
                    "age",
                    "histogram",
                    json!({
                        "buckets": [
                            {"lower": 0, "upper": 18, "count": 7500, "distinct": 18},
                            {"lower": 18, "upper": 35, "count": 52500, "distinct": 17},
                            {"lower": 35, "upper": 50, "count": 45000, "distinct": 15},
                            {"lower": 50, "upper": 65, "count": 30000, "distinct": 15},
                            {"lower": 65, "upper": 100, "count": 15000, "distinct": 35}
                        ],
                        "total_count": 150000
                    }),
                ),
            ],
            size_bytes: Some(78643200), // 75 MB
        };

        catalog
            .set_table_statistics(None, "optimizer_stats_table", updated_stats)
            .unwrap();

        let snapshot = catalog.current_snapshot().unwrap();
        let retrieved = catalog
            .table_statistics("optimizer_stats_table", snapshot)
            .unwrap()
            .expect("Updated stats should exist");

        assert_eq!(retrieved.row_count, 150000, "Row count should be updated");
        assert_eq!(retrieved.column_statistics.len(), 2, "Only 2 columns now");
        assert_eq!(
            retrieved.size_bytes,
            Some(78643200),
            "Size should be updated"
        );
    } // Drop catalog

    // ========== SESSION 4: Verify updates persisted ==========
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        let snapshot = catalog.current_snapshot().unwrap();
        let retrieved = catalog
            .table_statistics("optimizer_stats_table", snapshot)
            .unwrap()
            .expect("Updated stats should persist");

        assert_eq!(
            retrieved.row_count, 150000,
            "Updated row count should persist"
        );
        assert_eq!(
            retrieved.column_statistics.len(),
            2,
            "Updated column count should persist"
        );
        assert_eq!(
            retrieved.size_bytes,
            Some(78643200),
            "Updated size should persist"
        );

        // Verify old stats were replaced
        assert!(
            retrieved
                .column_statistics
                .iter()
                .all(|s| s.name == "id" || s.name == "age"),
            "Only id and age columns should remain"
        );
        assert!(
            retrieved
                .column_statistics
                .iter()
                .all(|s| s.name != "user_id"),
            "user_id column should be gone"
        );

        println!("✅ INTEGRATION TEST PASSED: Statistics updates persist across sessions!");
    }
}
