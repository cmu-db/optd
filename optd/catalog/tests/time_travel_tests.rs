//! Time-travel query tests.

use optd_catalog::{Catalog, DuckLakeCatalog, RegisterTableRequest};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_catalog() -> (TempDir, DuckLakeCatalog) {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    (temp_dir, catalog)
}

#[test]
fn test_get_table_at_snapshot_basic() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let snapshot_before = catalog.current_snapshot().unwrap();

    let request = RegisterTableRequest {
        table_name: "users".to_string(),
        schema_name: None,
        location: "/data/users.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    let snapshot_after = catalog.current_snapshot().unwrap();

    // Verify snapshot incremented by exactly 1
    assert_eq!(snapshot_after.0, snapshot_before.0 + 1);

    // Table should not exist before creation
    let result = catalog
        .get_external_table_at_snapshot(None, "users", snapshot_before.0)
        .unwrap();
    assert!(result.is_none());

    // Table should exist after creation
    let result = catalog
        .get_external_table_at_snapshot(None, "users", snapshot_after.0)
        .unwrap();
    let table = result.expect("Table should exist at snapshot after creation");
    assert_eq!(table.table_name, "users");
    assert_eq!(table.location, "/data/users.parquet");
    assert_eq!(table.file_format, "PARQUET");
}

#[test]
fn test_list_tables_at_snapshot() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let snapshot0 = catalog.current_snapshot().unwrap();

    let request1 = RegisterTableRequest {
        table_name: "table1".to_string(),
        schema_name: None,
        location: "/data/table1.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request1).unwrap();

    let snapshot1 = catalog.current_snapshot().unwrap();
    assert_eq!(snapshot1.0, snapshot0.0 + 1);

    let request2 = RegisterTableRequest {
        table_name: "table2".to_string(),
        schema_name: None,
        location: "/data/table2.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request2).unwrap();

    let snapshot2 = catalog.current_snapshot().unwrap();
    assert_eq!(snapshot2.0, snapshot1.0 + 1);

    // At snapshot0: no tables exist
    let tables = catalog
        .list_external_tables_at_snapshot(None, snapshot0.0)
        .unwrap();
    assert_eq!(tables.len(), 0);

    // At snapshot1: only table1 exists
    let tables = catalog
        .list_external_tables_at_snapshot(None, snapshot1.0)
        .unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name, "table1");

    // At snapshot2: both tables exist
    let tables = catalog
        .list_external_tables_at_snapshot(None, snapshot2.0)
        .unwrap();
    assert_eq!(tables.len(), 2);
    let names: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();
    assert!(names.contains(&"table1"));
    assert!(names.contains(&"table2"));
}

#[test]
fn test_time_travel_after_drop() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let request = RegisterTableRequest {
        table_name: "orders".to_string(),
        schema_name: None,
        location: "/data/orders.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    let snapshot_after_create = catalog.current_snapshot().unwrap();

    catalog.drop_external_table(None, "orders").unwrap();

    let snapshot_after_drop = catalog.current_snapshot().unwrap();
    assert_eq!(snapshot_after_drop.0, snapshot_after_create.0 + 1);

    // Table exists at creation snapshot
    let result = catalog
        .get_external_table_at_snapshot(None, "orders", snapshot_after_create.0)
        .unwrap();
    let table = result.expect("Table should exist before drop");
    assert_eq!(table.table_name, "orders");
    assert_eq!(table.begin_snapshot, snapshot_after_create.0);
    assert_eq!(table.end_snapshot, Some(snapshot_after_drop.0));

    // Table does not exist at drop snapshot
    let result = catalog
        .get_external_table_at_snapshot(None, "orders", snapshot_after_drop.0)
        .unwrap();
    assert!(result.is_none());

    // Table does not exist in current snapshot
    let result = catalog.get_external_table(None, "orders").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_list_tables_excludes_dropped() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let snapshot0 = catalog.current_snapshot().unwrap();

    // Create three tables
    for i in 1..=3 {
        let request = RegisterTableRequest {
            table_name: format!("table{}", i),
            schema_name: None,
            location: format!("/data/table{}.parquet", i),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        catalog.register_external_table(request).unwrap();
    }

    let snapshot_all = catalog.current_snapshot().unwrap();
    assert_eq!(snapshot_all.0, snapshot0.0 + 3);

    catalog.drop_external_table(None, "table2").unwrap();

    let snapshot_after_drop = catalog.current_snapshot().unwrap();
    assert_eq!(snapshot_after_drop.0, snapshot_all.0 + 1);

    // Current list excludes dropped table
    let tables = catalog.list_external_tables(None).unwrap();
    assert_eq!(tables.len(), 2);
    let names: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();
    assert!(names.contains(&"table1"));
    assert!(names.contains(&"table3"));
    assert!(!names.contains(&"table2"));

    // Historical list includes all tables
    let tables = catalog
        .list_external_tables_at_snapshot(None, snapshot_all.0)
        .unwrap();
    assert_eq!(tables.len(), 3);
}

#[test]
fn test_list_snapshots() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let initial_snapshots = catalog.list_snapshots().unwrap();
    let initial_count = initial_snapshots.len();
    assert_eq!(initial_count, 1, "Should start with exactly one snapshot");
    assert_eq!(initial_snapshots[0].id.0, 0);

    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: Some("main".to_string()),
        location: "file:///tmp/test.parquet".to_string(),
        file_format: "parquet".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    catalog.register_external_table(request).unwrap();

    let snapshots_after_create = catalog.list_snapshots().unwrap();
    assert_eq!(snapshots_after_create.len(), 2);
    assert_eq!(snapshots_after_create[0].id.0, 0);
    assert_eq!(snapshots_after_create[1].id.0, 1);

    catalog
        .drop_external_table(Some("main"), "test_table")
        .unwrap();

    let snapshots_after_drop = catalog.list_snapshots().unwrap();
    assert_eq!(snapshots_after_drop.len(), 3);
    assert_eq!(snapshots_after_drop[0].id.0, 0);
    assert_eq!(snapshots_after_drop[1].id.0, 1);
    assert_eq!(snapshots_after_drop[2].id.0, 2);

    // Verify each snapshot has valid metadata
    for snapshot in &snapshots_after_drop {
        assert!(snapshot.schema_version >= 0);
        assert!(snapshot.next_catalog_id >= 0);
        assert!(snapshot.next_file_id >= 0);
    }
}
