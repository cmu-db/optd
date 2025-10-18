use std::sync::Arc;

use datafusion::{execution::SendableRecordBatchStream, physical_plan::memory::MemoryStream};
use duckdb::{
    Connection, DuckdbConnectionManager as DuckDBManager,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
};
use r2d2::ManageConnection;
use snafu::{ResultExt, prelude::*};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Connection to DuckDB failed: {}", source))]
    ConnectionError { source: duckdb::Error },
    #[snafu(display("Failed to get connection from pool: {}", source))]
    PoolError { source: r2d2::Error },
    #[snafu(display("Invalid database path: {}", path))]
    InvalidPathError { path: Arc<str> },
    #[snafu(display("Arrow query failed: {}", source))]
    ArrowError {
        source: duckdb::arrow::error::ArrowError,
    },
    #[snafu(display("DataFusion error: {}", source))]
    DataFusionError {
        source: datafusion::error::DataFusionError,
    },
    #[snafu(display("Other error: {}", details))]
    Other { details: Arc<str> },
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Error::DataFusionError { source: err }
    }
}

impl From<duckdb::Error> for Error {
    fn from(err: duckdb::Error) -> Self {
        Error::ConnectionError { source: err }
    }
}

#[derive(Debug)]
pub enum ConnectionMode {
    Memory,
    File,
}

pub struct DuckLakeConnectionBuilder {
    meta_name: Arc<str>,
    path: Arc<str>,
    mode: ConnectionMode,
    manager: DuckDBManager,
}

impl std::fmt::Debug for DuckLakeConnectionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DuckLakeConnectionBuilder {{ name: {}, path: {}, mode: {:?} }}",
            self.meta_name, self.path, self.mode
        )
    }
}

impl DuckLakeConnectionBuilder {
    // Default constants
    pub const DEFAULT_LAKE_NAME: &str = "meta_lake";

    pub fn memory() -> Result<Self, Error> {
        let manager = DuckDBManager::memory().context(ConnectionSnafu)?;
        Ok(Self {
            meta_name: Arc::from(Self::DEFAULT_LAKE_NAME),
            path: Arc::from(":memory:"),
            mode: ConnectionMode::Memory,
            manager,
        })
    }

    pub fn file(path: &str) -> Result<Self, Error> {
        let manager = DuckDBManager::file(path).context(ConnectionSnafu)?;
        Ok(Self {
            meta_name: Arc::from(Self::DEFAULT_LAKE_NAME),
            path: Arc::from(path),
            mode: ConnectionMode::File,
            manager,
        })
    }

    pub fn meta_name(&mut self, name: &str) -> &Self {
        self.meta_name = Arc::from(name);
        self
    }

    pub fn get_meta_name(&self) -> &str {
        self.meta_name.as_ref()
    }

    pub fn get_path(&self) -> &str {
        self.path.as_ref()
    }

    pub fn get_mode(&self) -> &ConnectionMode {
        &self.mode
    }

    pub fn connect(&self) -> Result<Connection, Error> {
        let mut connection = self.manager.connect().context(ConnectionSnafu)?;
        self.ducklake(&mut connection)?;
        Ok(connection)
    }

    fn ducklake(&self, connection: &mut Connection) -> Result<(), Error> {
        let setup_query = match self.mode {
            ConnectionMode::Memory => format!(
                r#"
                    INSTALL ducklake;
                    LOAD ducklake;
                    ATTACH 'ducklake:metadata.ducklake' AS {name};
                    USE {name};
                "#,
                name = self.meta_name.as_ref(),
            ),
            ConnectionMode::File => format!(
                r#"
                    INSTALL ducklake;
                    LOAD ducklake;
                    ATTACH 'ducklake:metadata.ducklake' AS {name} (DATA_PATH '{path}');
                    USE {name};
                "#,
                name = self.meta_name.as_ref(),
                path = self.path.as_ref()
            ),
        };

        connection
            .execute_batch(setup_query.as_str())
            .context(ConnectionSnafu)?;
        Ok(())
    }

    pub fn initialize_schema(&self, connection: &Connection) -> Result<(), Error> {
        // Create tables for storing stats metadata
        connection.execute_batch(
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.ducklake_table_column_adv_stats (
                    column_id BIGINT,
                    begin_snapshot BIGINT,
                    end_snapshot BIGINT,
                    table_id BIGINT,
                    stats_type VARCHAR,
                    payload TEXT,
                    PRIMARY KEY (column_id, begin_snapshot, stats_type)
                );

                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.optd_query (
                    query_id BIGINT PRIMARY KEY,
                    query_string TEXT,
                    root_group_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.optd_query_instance (
                    query_instance_id BIGINT PRIMARY KEY,
                    query_id BIGINT,
                    creation_time BIGINT,
                    snapshot_id BIGINT,
                    FOREIGN KEY (query_id) REFERENCES __ducklake_metadata_{name}.main.optd_query(query_id)
                );

                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.optd_group (
                    group_id BIGINT,
                    begin_snapshot BIGINT,
                    end_snapshot BIGINT,
                    PRIMARY KEY (group_id, begin_snapshot)
                );

                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.optd_group_stats (
                    group_id BIGINT,
                    begin_snapshot BIGINT,
                    end_snapshot BIGINT,
                    stats_type VARCHAR,
                    payload TEXT,
                    PRIMARY KEY (group_id, begin_snapshot, stats_type)
                );

                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.optd_execution_subplan_feedback (
                    group_id BIGINT,
                    begin_snapshot BIGINT,
                    end_snapshot BIGINT,
                    stats_type VARCHAR,
                    payload TEXT,
                    PRIMARY KEY (group_id, begin_snapshot, stats_type)
                );

                CREATE TABLE IF NOT EXISTS __ducklake_metadata_{name}.main.optd_subplan_scalar_feedback (
                    scalar_id BIGINT,
                    group_id BIGINT,
                    stats_type VARCHAR,
                    payload TEXT,
                    query_instance_id BIGINT,
                    PRIMARY KEY (scalar_id, group_id, stats_type, query_instance_id)
                );

                CREATE INDEX IF NOT EXISTS idx_table_stats_snapshot
                    ON __ducklake_metadata_{name}.main.ducklake_table_column_adv_stats(begin_snapshot, end_snapshot);
            "#,
                name = self.meta_name.as_ref()
            )
            .as_str(),
        )
        .context(ConnectionSnafu)?;

        Ok(())
    }
}

impl Default for DuckLakeConnectionBuilder {
    fn default() -> Self {
        Self::memory().expect("Failed to create default DuckLakeConnectionBuilder")
    }
}

pub async fn query(
    connection: &Connection,
    sql: &str,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
) -> Result<SendableRecordBatchStream, Error> {
    let mut stmt = connection.prepare(sql).context(ConnectionSnafu)?;

    let rbs = stmt.query_arrow([])?.collect::<Vec<RecordBatch>>();
    let stream = MemoryStream::try_new(rbs, schema, projection)?;
    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn test_ducklake_connection() {
        let path = "./test_ducklake.db";
        let ducklake_conn =
            DuckLakeConnectionBuilder::file(path).expect("Failed to create DuckLakeConnection");
        assert_eq!(ducklake_conn.get_path(), path);

        assert_eq!(
            matches!(ducklake_conn.get_mode(), ConnectionMode::File),
            true
        );

        assert_eq!(ducklake_conn.get_meta_name(), "meta_lake");

        {
            let conn = ducklake_conn.connect().expect("Failed to get connection");
            assert!(conn.execute_batch("SELECT 1;").is_ok());

            let mut stmt = conn
                .prepare("select name from (show all tables);")
                .expect("Failed to prepare show tables statement");

            let rows = stmt
                .query_map([], |row| row.get::<usize, String>(0))
                .expect("Failed to execute show tables query");

            let expected = vec![
                "ducklake_column",
                "ducklake_column_mapping",
                "ducklake_column_tag",
                "ducklake_data_file",
                "ducklake_delete_file",
                "ducklake_file_column_stats",
                "ducklake_file_partition_value",
                "ducklake_files_scheduled_for_deletion",
                "ducklake_inlined_data_tables",
                "ducklake_metadata",
                "ducklake_name_mapping",
                "ducklake_partition_column",
                "ducklake_partition_info",
                "ducklake_schema",
                "ducklake_schema_versions",
                "ducklake_snapshot",
                "ducklake_snapshot_changes",
                "ducklake_table",
                "ducklake_table_column_stats",
                "ducklake_table_stats",
                "ducklake_tag",
                "ducklake_view",
            ];

            for (i, row) in rows.enumerate() {
                let table_name = row.expect("Failed to get table name");
                assert_eq!(table_name, expected[i]);
            }
        }

        {
            let conn = ducklake_conn.connect().expect("Failed to get connection");
            ducklake_conn
                .initialize_schema(&conn)
                .expect("Failed to initialize schema");

            let mut stmt = conn
                .prepare("select name from (show all tables);")
                .expect("Failed to prepare show tables statement");

            let rows = stmt
                .query_map([], |row| row.get::<usize, String>(0))
                .expect("Failed to execute show tables query");

            let expected = vec![
                "ducklake_column",
                "ducklake_column_mapping",
                "ducklake_column_tag",
                "ducklake_data_file",
                "ducklake_delete_file",
                "ducklake_file_column_stats",
                "ducklake_file_partition_value",
                "ducklake_files_scheduled_for_deletion",
                "ducklake_inlined_data_tables",
                "ducklake_metadata",
                "ducklake_name_mapping",
                "ducklake_partition_column",
                "ducklake_partition_info",
                "ducklake_schema",
                "ducklake_schema_versions",
                "ducklake_snapshot",
                "ducklake_snapshot_changes",
                "ducklake_table",
                "ducklake_table_column_adv_stats",
                "ducklake_table_column_stats",
                "ducklake_table_stats",
                "ducklake_tag",
                "ducklake_view",
                "optd_execution_subplan_feedback",
                "optd_group",
                "optd_group_stats",
                "optd_query",
                "optd_subplan_scalar_feedback",
            ];

            for (i, row) in rows.enumerate() {
                let table_name = row.expect("Failed to get table name");
                assert_eq!(table_name, expected[i]);
            }
        }

        {
            let conn = ducklake_conn.connect().expect("Failed to get connection");
            conn.execute_batch("CREATE TABLE IF NOT EXISTS test (id INTEGER, name VARCHAR);")
                .expect("Failed to create table");
            conn.execute_batch("INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob');")
                .expect("Failed to insert data");
        }

        {
            let conn = ducklake_conn.connect().expect("Failed to get connection");
            let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                datafusion::arrow::datatypes::Field::new(
                    "id",
                    datafusion::arrow::datatypes::DataType::Int32,
                    false,
                ),
                datafusion::arrow::datatypes::Field::new(
                    "name",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    false,
                ),
            ]));

            let rbs = query(&conn, "SELECT * FROM test;", schema, None)
                .await
                .expect("Failed to execute query");

            let schema_ref = rbs.schema();
            assert_eq!(schema_ref.fields().len(), 2);
            assert_eq!(schema_ref.field(0).name(), "id");
            assert_eq!(schema_ref.field(1).name(), "name");

            let batches: Vec<_> = rbs
                .try_collect()
                .await
                .expect("Failed to collect record batches");

            assert_eq!(batches.len(), 1);
            let batch = &batches[0];
            assert_eq!(batch.num_rows(), 2);
        }
    }
}
