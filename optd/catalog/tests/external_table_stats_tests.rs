// Tests external table statistics using TableStatistics and ColumnStatistics

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
    };
    catalog
        .set_table_statistics(None, "test_table", stats)
        .unwrap();

    // Get statistics
    let retrieved_stats = catalog
        .get_table_statistics_manual(None, "test_table")
        .unwrap();
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
    };
    catalog.set_table_statistics(None, "users", stats).unwrap();

    // Get and verify statistics
    let retrieved_stats = catalog
        .get_table_statistics_manual(None, "users")
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
    };
    catalog
        .set_table_statistics(None, "test_table", stats1)
        .unwrap();

    // Verify initial statistics
    let retrieved1 = catalog
        .get_table_statistics_manual(None, "test_table")
        .unwrap()
        .unwrap();
    assert_eq!(retrieved1.row_count, 100);

    // Update statistics
    let stats2 = TableStatistics {
        row_count: 200,
        column_statistics: vec![],
    };
    catalog
        .set_table_statistics(None, "test_table", stats2)
        .unwrap();

    // Verify updated statistics
    let retrieved2 = catalog
        .get_table_statistics_manual(None, "test_table")
        .unwrap()
        .unwrap();
    assert_eq!(retrieved2.row_count, 200);
}

#[test]
fn test_get_statistics_for_nonexistent_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let result = catalog.get_table_statistics_manual(None, "nonexistent");
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

    let result = catalog.get_table_statistics_manual(None, "test_table");
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
        };
        catalog
            .set_table_statistics(None, "persistent_table", stats)
            .unwrap();
    } // catalog dropped here

    // Second session: reconnect and verify statistics persist
    {
        let mut catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();

        let retrieved = catalog
            .get_table_statistics_manual(None, "persistent_table")
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
    };
    catalog
        .set_table_statistics(None, "evolving_table", initial_stats)
        .unwrap();

    // Verify initial
    let retrieved_initial = catalog
        .get_table_statistics_manual(None, "evolving_table")
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
    };
    catalog
        .set_table_statistics(None, "evolving_table", updated_stats)
        .unwrap();

    // Verify update
    let retrieved_updated = catalog
        .get_table_statistics_manual(None, "evolving_table")
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
    };
    catalog
        .set_table_statistics(None, "partial_stats_table", stats)
        .unwrap();

    // Verify
    let retrieved = catalog
        .get_table_statistics_manual(None, "partial_stats_table")
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
    };
    catalog
        .set_table_statistics(None, "empty_col_stats", stats)
        .unwrap();

    // Verify
    let retrieved = catalog
        .get_table_statistics_manual(None, "empty_col_stats")
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
    };
    catalog
        .set_table_statistics(None, "products", stats)
        .unwrap();

    // Get and verify all stat types are preserved
    let retrieved_stats = catalog
        .get_table_statistics_manual(None, "products")
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
