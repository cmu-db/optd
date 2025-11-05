mod optd_catalog;
mod optd_table;

use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // Create Sqlite database file to hold the catalog
    const SQLITE_DB_PATH: &str = "catalog.db";

    // Set connect options
    let connect_options = SqliteConnectOptions::new()
        .filename(SQLITE_DB_PATH)
        .create_if_missing(true);

    // Connect with SqlX
    let pool = SqlitePool::connect_with(connect_options)
        .await
        .expect("Failed to connect to the SQLite database");

    // Set the metadata catalog name
    const METADATA_CATALOG: &str = "catalog";

    // Execute the given Sql queries to create the catalog
    let mut create_catalog_queries = vec![
        // "CREATE TABLE () IF NOT EXISTS {METADATA_CATALOG};",
        "CREATE TABLE {METADATA_CATALOG}_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);",
        "CREATE TABLE {METADATA_CATALOG}_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);",
        "CREATE TABLE {METADATA_CATALOG}_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, partial_file_info VARCHAR, mapping_id BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_file_column_statistics(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN);",
        "CREATE TABLE {METADATA_CATALOG}_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ);",
        "CREATE TABLE {METADATA_CATALOG}_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT);",
        "CREATE TABLE {METADATA_CATALOG}_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR);",
        "CREATE TABLE {METADATA_CATALOG}_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT);",
        "INSERT INTO {METADATA_CATALOG}_snapshot VALUES (0, current_timestamp, 0, 1, 0);",
        "INSERT INTO {METADATA_CATALOG}_snapshot_changes VALUES (0, 'created_schema:\"main\"');",
        //"INSERT INTO {METADATA_CATALOG}_metadata (key, value) VALUES ('version', '0.2'), ('created_by', 'DuckDB %s'), ('data_path', %s), ('encrypted', '%s');"
    ];

    let set_uuid_query = format!(
        "UPDATE {METADATA_CATALOG}_schema SET schema_uuid = '{}' WHERE schema_id = 0;",
        Uuid::new_v4()
    );

    create_catalog_queries.push(set_uuid_query.as_str());

    // Format the queries with the metadata catalog name
    let formatted_query = create_catalog_queries
        .iter()
        .map(|query| query.replace("{METADATA_CATALOG}", METADATA_CATALOG));

    for query in formatted_query {
        println!("Executing query: {}", query);
        sqlx::query(&query)
            .execute(&pool)
            .await
            .expect("Failed to execute query");

        println!("Query executed successfully.");
    }

    // Close the connection
    pool.close().await;

    Ok(())
}
