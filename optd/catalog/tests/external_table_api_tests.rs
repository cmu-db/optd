use optd_catalog::{Catalog, DuckLakeCatalog, RegisterTableRequest};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_catalog() -> (TempDir, DuckLakeCatalog) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let catalog = DuckLakeCatalog::try_new(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )
    .unwrap();
    (temp_dir, catalog)
}

#[test]
fn test_register_external_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let mut options = HashMap::new();
    options.insert("has_header".to_string(), "true".to_string());
    options.insert("delimiter".to_string(), ",".to_string());

    let request = RegisterTableRequest {
        table_name: "users".to_string(),
        schema_name: None,
        location: "/data/users.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: Some("gzip".to_string()),
        options: options.clone(),
    };

    let metadata = catalog.register_external_table(request).unwrap();

    assert_eq!(metadata.table_name, "users");
    assert_eq!(metadata.location, "/data/users.csv");
    assert_eq!(metadata.file_format, "CSV");
    assert_eq!(metadata.compression, Some("gzip".to_string()));
    assert_eq!(metadata.options.get("has_header").unwrap(), "true");
    assert_eq!(metadata.options.get("delimiter").unwrap(), ",");
    assert_eq!(metadata.end_snapshot, None); // Active table
}

#[test]
fn test_register_multiple_tables_increments_id() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let request1 = RegisterTableRequest {
        table_name: "table1".to_string(),
        schema_name: None,
        location: "/data/table1.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    let request2 = RegisterTableRequest {
        table_name: "table2".to_string(),
        schema_name: None,
        location: "/data/table2.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    let metadata1 = catalog.register_external_table(request1).unwrap();
    let metadata2 = catalog.register_external_table(request2).unwrap();

    assert_eq!(metadata1.table_id, 1);
    assert_eq!(metadata2.table_id, 2);
}

#[test]
fn test_get_external_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Register a table first
    let mut options = HashMap::new();
    options.insert("format_version".to_string(), "2".to_string());

    let request = RegisterTableRequest {
        table_name: "products".to_string(),
        schema_name: None,
        location: "/data/products.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: Some("snappy".to_string()),
        options: options.clone(),
    };

    let registered = catalog.register_external_table(request).unwrap();

    // Retrieve it
    let retrieved = catalog
        .get_external_table(None, "products")
        .unwrap()
        .expect("Table should exist");

    assert_eq!(retrieved.table_id, registered.table_id);
    assert_eq!(retrieved.table_name, "products");
    assert_eq!(retrieved.location, "/data/products.parquet");
    assert_eq!(retrieved.file_format, "PARQUET");
    assert_eq!(retrieved.compression, Some("snappy".to_string()));
    assert_eq!(retrieved.options.get("format_version").unwrap(), "2");
}

#[test]
fn test_get_nonexistent_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let result = catalog.get_external_table(None, "nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_list_external_tables() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Register multiple tables
    let tables = vec![
        ("orders", "/data/orders.parquet"),
        ("customers", "/data/customers.csv"),
        ("products", "/data/products.json"),
    ];

    for (name, location) in &tables {
        let request = RegisterTableRequest {
            table_name: name.to_string(),
            schema_name: None,
            location: location.to_string(),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        catalog.register_external_table(request).unwrap();
    }

    // List all tables
    let listed = catalog.list_external_tables(None).unwrap();

    assert_eq!(listed.len(), 3);

    // Should be ordered by table_name (alphabetically)
    assert_eq!(listed[0].table_name, "customers");
    assert_eq!(listed[1].table_name, "orders");
    assert_eq!(listed[2].table_name, "products");
}

#[test]
fn test_list_external_tables_empty() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let listed = catalog.list_external_tables(None).unwrap();
    assert!(listed.is_empty());
}

#[test]
fn test_drop_external_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Register a table
    let request = RegisterTableRequest {
        table_name: "temp_table".to_string(),
        schema_name: None,
        location: "/data/temp.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    catalog.register_external_table(request).unwrap();

    // Verify it exists
    let before_drop = catalog.get_external_table(None, "temp_table").unwrap();
    assert!(before_drop.is_some());

    // Drop it
    catalog.drop_external_table(None, "temp_table").unwrap();

    // Verify it's gone
    let after_drop = catalog.get_external_table(None, "temp_table").unwrap();
    assert!(after_drop.is_none());
}

#[test]
fn test_drop_nonexistent_table_fails() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let result = catalog.drop_external_table(None, "nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_list_excludes_dropped_tables() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Register 3 tables
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

    // Drop the middle one
    catalog.drop_external_table(None, "table2").unwrap();

    // List should only show 2
    let listed = catalog.list_external_tables(None).unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].table_name, "table1");
    assert_eq!(listed[1].table_name, "table3");
}

#[test]
fn test_external_table_metadata_persists_across_connections() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let db_str = db_path.to_str().unwrap().to_string();
    let metadata_str = metadata_path.to_str().unwrap().to_string();

    // Register tables in first connection
    {
        let mut catalog = DuckLakeCatalog::try_new(Some(&db_str), Some(&metadata_str)).unwrap();

        let request = RegisterTableRequest {
            table_name: "persistent_table".to_string(),
            schema_name: None,
            location: "/data/persistent.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            compression: Some("zstd".to_string()),
            options: {
                let mut opts = HashMap::new();
                opts.insert("key1".to_string(), "value1".to_string());
                opts
            },
        };

        catalog.register_external_table(request).unwrap();
    } // Connection dropped

    // Verify in second connection
    {
        let mut catalog = DuckLakeCatalog::try_new(Some(&db_str), Some(&metadata_str)).unwrap();

        let retrieved = catalog
            .get_external_table(None, "persistent_table")
            .unwrap()
            .expect("Table should persist");

        assert_eq!(retrieved.table_name, "persistent_table");
        assert_eq!(retrieved.location, "/data/persistent.parquet");
        assert_eq!(retrieved.compression, Some("zstd".to_string()));
        assert_eq!(retrieved.options.get("key1").unwrap(), "value1");
    }
}

#[test]
fn test_options_empty_allowed() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let request = RegisterTableRequest {
        table_name: "no_options".to_string(),
        schema_name: None,
        location: "/data/simple.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    let metadata = catalog.register_external_table(request).unwrap();
    assert!(metadata.options.is_empty());

    let retrieved = catalog
        .get_external_table(None, "no_options")
        .unwrap()
        .unwrap();
    assert!(retrieved.options.is_empty());
}
