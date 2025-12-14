use optd_catalog::DuckLakeCatalog;
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
fn test_external_table_metadata_persists_across_connections() {
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
