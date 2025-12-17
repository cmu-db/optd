//! External table registration, retrieval, and management tests.
//! Tests both direct API calls and service layer (async RPC).

use optd_catalog::{Catalog, CatalogService, DuckLakeCatalog, RegisterTableRequest};
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

fn create_test_service_setup() -> (
    TempDir,
    optd_catalog::CatalogServiceHandle,
    CatalogService<DuckLakeCatalog>,
) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let (service, handle) = CatalogService::try_new_from_location(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )
    .unwrap();
    (temp_dir, handle, service)
}

// ============================================================================
// Direct API Tests
// ============================================================================

#[test]
fn test_register_and_retrieve_external_table() {
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
    assert_eq!(metadata.end_snapshot, None); // Active

    // Retrieve
    let retrieved = catalog.get_external_table(None, "users").unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.table_name, "users");
    assert_eq!(retrieved.location, "/data/users.csv");
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

    // External tables use negative IDs
    assert_eq!(metadata1.table_id, -1);
    assert_eq!(metadata2.table_id, -2);
}

#[test]
fn test_multiple_tables() {
    let (_temp_dir, mut catalog) = create_test_catalog();

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

    let tables = catalog.list_external_tables(None).unwrap();
    assert_eq!(tables.len(), 3);
}

#[test]
fn test_drop_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let request = RegisterTableRequest {
        table_name: "temp".to_string(),
        schema_name: None,
        location: "/data/temp.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    catalog.register_external_table(request).unwrap();
    assert!(catalog.get_external_table(None, "temp").unwrap().is_some());

    catalog.drop_external_table(None, "temp").unwrap();
    assert!(catalog.get_external_table(None, "temp").unwrap().is_none());
}

#[test]
fn test_nonexistent_table() {
    let (_temp_dir, mut catalog) = create_test_catalog();
    let result = catalog.get_external_table(None, "nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_list_empty_catalog() {
    let (_temp_dir, mut catalog) = create_test_catalog();
    let tables = catalog.list_external_tables(None).unwrap();
    assert_eq!(tables.len(), 0);
}

#[test]
fn test_list_excludes_dropped_tables() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    // Register two tables
    for i in 1..=2 {
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

    // Drop one table
    catalog.drop_external_table(None, "table1").unwrap();

    // List should only show active table
    let tables = catalog.list_external_tables(None).unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name, "table2");
}

#[test]
fn test_metadata_persists_across_connections() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // First connection - register table
    {
        let mut catalog = DuckLakeCatalog::try_new(
            Some(db_path.to_str().unwrap()),
            Some(metadata_path.to_str().unwrap()),
        )
        .unwrap();

        let request = RegisterTableRequest {
            table_name: "persistent".to_string(),
            schema_name: None,
            location: "/data/persistent.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        catalog.register_external_table(request).unwrap();
    }

    // Second connection - verify table exists
    {
        let mut catalog = DuckLakeCatalog::try_new(
            Some(db_path.to_str().unwrap()),
            Some(metadata_path.to_str().unwrap()),
        )
        .unwrap();

        let retrieved = catalog.get_external_table(None, "persistent").unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().table_name, "persistent");
    }
}

#[test]
fn test_empty_options_allowed() {
    let (_temp_dir, mut catalog) = create_test_catalog();

    let request = RegisterTableRequest {
        table_name: "simple".to_string(),
        schema_name: None,
        location: "/data/simple.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(), // Empty options
    };

    let metadata = catalog.register_external_table(request).unwrap();
    assert!(metadata.options.is_empty());
}

// ============================================================================
// Service Layer Tests
// ============================================================================

#[tokio::test]
async fn test_service_register_and_retrieve() {
    let (_temp_dir, handle, service) = create_test_service_setup();

    tokio::spawn(async move {
        service.run().await;
    });

    let request = RegisterTableRequest {
        table_name: "users".to_string(),
        schema_name: None,
        location: "/data/users.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    let metadata = handle.register_external_table(request).await.unwrap();
    assert_eq!(metadata.table_name, "users");

    let retrieved = handle.get_external_table(None, "users").await.unwrap();
    assert!(retrieved.is_some());

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_list_tables() {
    let (_temp_dir, handle, service) = create_test_service_setup();

    tokio::spawn(async move {
        service.run().await;
    });

    for i in 1..=2 {
        let request = RegisterTableRequest {
            table_name: format!("table{}", i),
            schema_name: None,
            location: format!("/data/t{}.parquet", i),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        handle.register_external_table(request).await.unwrap();
    }

    let tables = handle.list_external_tables(None).await.unwrap();
    assert_eq!(tables.len(), 2);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_drop_table() {
    let (_temp_dir, handle, service) = create_test_service_setup();

    tokio::spawn(async move {
        service.run().await;
    });

    let request = RegisterTableRequest {
        table_name: "temp".to_string(),
        schema_name: None,
        location: "/data/temp.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    handle.register_external_table(request).await.unwrap();
    handle.drop_external_table(None, "temp").await.unwrap();

    let retrieved = handle.get_external_table(None, "temp").await.unwrap();
    assert!(retrieved.is_none());

    handle.shutdown().await.unwrap();
}

// ============================================================================
// Metadata Infrastructure Tests
// ============================================================================

#[test]
fn test_external_table_schema_created() {
    let (_temp_dir, catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    let tables_exist: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_name IN ('optd_external_table', 'optd_external_table_options')
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(tables_exist, 2);
}

#[test]
fn test_external_table_indexes_created() {
    let (_temp_dir, catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    let indexes_exist: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*) FROM duckdb_indexes() 
            WHERE index_name IN ('idx_optd_external_table_schema', 'idx_optd_external_table_snapshot')
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(indexes_exist, 2);
}

#[test]
fn test_external_table_metadata_persists_at_sql_level() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let db_str = db_path.to_str().unwrap().to_string();
    let metadata_str = metadata_path.to_str().unwrap().to_string();

    let (_schema_id, _snapshot_id) = {
        let catalog = DuckLakeCatalog::try_new(Some(&db_str), Some(&metadata_str)).unwrap();
        let conn = catalog.get_connection();

        let schema_id: i64 = conn
            .query_row(
                "SELECT schema_id FROM __ducklake_metadata_metalake.main.ducklake_schema WHERE schema_name = 'main'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let snapshot_id: i64 = conn
            .query_row(
                "SELECT MAX(snapshot_id) FROM __ducklake_metadata_metalake.main.ducklake_snapshot",
                [],
                |row| row.get(0),
            )
            .unwrap();

        conn.execute(
            r#"
            INSERT INTO __ducklake_metadata_metalake.main.optd_external_table 
            (table_id, schema_id, table_name, location, file_format, begin_snapshot)
            VALUES (1, ?, 'test_table', '/data/test.parquet', 'PARQUET', ?)
            "#,
            [schema_id, snapshot_id],
        )
        .unwrap();

        conn.execute(
            r#"
            INSERT INTO __ducklake_metadata_metalake.main.optd_external_table_options
            (table_id, option_key, option_value)
            VALUES (1, 'compression', 'snappy'), (1, 'row_group_size', '1024')
            "#,
            [],
        )
        .unwrap();

        (schema_id, snapshot_id)
    };

    {
        let catalog = DuckLakeCatalog::try_new(Some(&db_str), Some(&metadata_str)).unwrap();
        let conn = catalog.get_connection();

        let (retrieved_table_name, retrieved_location, retrieved_format): (String, String, String) =
            conn.query_row(
                r#"
                SELECT table_name, location, file_format 
                FROM __ducklake_metadata_metalake.main.optd_external_table 
                WHERE table_id = 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(retrieved_table_name, "test_table");
        assert_eq!(retrieved_location, "/data/test.parquet");
        assert_eq!(retrieved_format, "PARQUET");

        let option_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.optd_external_table_options WHERE table_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(option_count, 2);
    }
}

#[test]
fn test_external_table_soft_delete_with_end_snapshot() {
    let (_temp_dir, catalog) = create_test_catalog();
    let conn = catalog.get_connection();

    let schema_id: i64 = conn
        .query_row(
            "SELECT schema_id FROM __ducklake_metadata_metalake.main.ducklake_schema WHERE schema_name = 'main'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    conn.execute(
        r#"
        INSERT INTO __ducklake_metadata_metalake.main.optd_external_table 
        (table_id, schema_id, table_name, location, file_format, begin_snapshot, end_snapshot)
        VALUES (1, ?, 'active_table', '/data/active.parquet', 'PARQUET', 1, NULL),
               (2, ?, 'deleted_table', '/data/deleted.parquet', 'PARQUET', 1, 5)
        "#,
        [schema_id, schema_id],
    )
    .unwrap();

    let active_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.optd_external_table WHERE end_snapshot IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(active_count, 1);

    let deleted_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.optd_external_table WHERE end_snapshot IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(deleted_count, 1);
}
