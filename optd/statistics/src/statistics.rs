use std::sync::Arc;

use duckdb::{
    Connection, Error as DuckDBError,
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    params,
    types::Null,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ResultExt, prelude::*};

const DEFAULT_METADATA_FILE: &str = "metadata.ducklake";

/// SQL query to fetch table statistics including column metadata and advanced stats at a specific snapshot.
const FETCH_TABLE_STATS_QUERY: &str = r#"
    SELECT 
        ts.table_id,
        dc.column_id,
        dc.column_name,
        dc.column_type,
        ts.record_count,
        ts.next_row_id,
        ts.file_size_bytes,
        tcas.stats_type,
        tcas.payload
    FROM __ducklake_metadata_metalake.main.ducklake_table_stats ts
    INNER JOIN __ducklake_metadata_metalake.main.ducklake_table dt ON ts.table_id = dt.table_id
    INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
    INNER JOIN __ducklake_metadata_metalake.main.ducklake_column dc ON dt.table_id = dc.table_id
    LEFT JOIN __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats tcas 
        ON dc.table_id = tcas.table_id 
        AND dc.column_id = tcas.column_id
        AND ? >= tcas.begin_snapshot 
        AND (? < tcas.end_snapshot OR tcas.end_snapshot IS NULL)
    WHERE 
        ds.schema_name = current_schema()
        AND dt.table_name = ?
        AND ts.record_count IS NOT NULL 
        AND ts.file_size_bytes IS NOT NULL
        AND ? >= dc.begin_snapshot 
        AND (? < dc.end_snapshot OR dc.end_snapshot IS NULL)
    ORDER BY ts.table_id, dc.column_id, tcas.stats_type;
"#;

/// SQL query to close an existing advanced statistics entry by setting its end_snapshot.
const UPDATE_ADV_STATS_QUERY: &str = r#"
    UPDATE __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
    SET end_snapshot = ?
    WHERE end_snapshot IS NULL
        AND stats_type = ?
        AND column_id = ?
        AND table_id = ?;
"#;

/// SQL query to insert a new advanced statistics entry.
const INSERT_ADV_STATS_QUERY: &str = r#"
    INSERT INTO __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
        (column_id, begin_snapshot, end_snapshot, table_id, stats_type, payload) 
    VALUES (?, ?, ?, ?, ?, ?);
"#;

/// SQL query to insert a new snapshot record.
const INSERT_SNAPSHOT_QUERY: &str = r#"
    INSERT INTO __ducklake_metadata_metalake.main.ducklake_snapshot
        (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) 
    VALUES (?, NOW(), ?, ?, ?);
"#;

/// SQL query to record a snapshot change in the change log.
const INSERT_SNAPSHOT_CHANGE_QUERY: &str = r#"
    INSERT INTO __ducklake_metadata_metalake.main.ducklake_snapshot_changes
        (snapshot_id, changes_made, author, commit_message, commit_extra_info)
    VALUES (?, ?, ?, ?, ?);
"#;

/// Error types for statistics operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database connection error: {}", source))]
    Connection { source: DuckDBError },
    #[snafu(display("Query execution failed: {}", source))]
    QueryExecution { source: DuckDBError },
    #[snafu(display("JSON serialization error: {}", source))]
    JsonSerialization { source: serde_json::Error },
    #[snafu(display("ARROW DataType conversion error: {}", source))]
    ArrowDataTypeConversion { source: duckdb::Error },
    #[snafu(display(
        "Get statistics failed for table: {}, column: {}, snapshot: {}",
        table,
        column,
        snapshot
    ))]
    GetStatsFailed {
        table: String,
        column: String,
        snapshot: i64,
    },
    #[snafu(display(
        "Group statistics not found for group_id: {}, stats_type: {}, snapshot: {}",
        group_id,
        stats_type,
        snapshot
    ))]
    GroupStatsNotFound {
        group_id: i64,
        stats_type: String,
        snapshot: i64,
    },
}

/// Internal struct representing a row from the table statistics query.
/// Used for collecting data before aggregating into TableStatistics.
struct TableColumnStatisticsEntry {
    table_id: i64,
    column_id: i64,
    column_name: String,
    column_type: String,
    record_count: i64,
    next_row_id: i64,
    file_size_bytes: i64,
    stats_type: Option<String>,
    payload: Option<String>,
}

/// Statistics for a table including row count and per-column statistics.
/// Main structure returned when querying table statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub row_count: usize,
    pub column_statistics: Vec<ColumnStatistics>,
}

impl FromIterator<Result<TableColumnStatisticsEntry, Error>> for TableStatistics {
    fn from_iter<T: IntoIterator<Item = Result<TableColumnStatisticsEntry, Error>>>(
        iter: T,
    ) -> Self {
        let mut row_flag = false;
        let mut row_count = 0;
        let mut column_statistics = Vec::new();

        // Stats will be ordered by table_id then column_id
        for TableColumnStatisticsEntry {
            table_id: _,
            column_id,
            column_name,
            column_type,
            record_count,
            next_row_id: _,
            file_size_bytes: _,
            stats_type,
            payload,
        } in iter.into_iter().flatten()
        {
            // Check if unique table/column combination
            if column_statistics
                .last()
                .is_none_or(|last: &ColumnStatistics| last.column_id != column_id)
            {
                // New column encountered
                column_statistics.push(ColumnStatistics::new(
                    column_id,
                    column_type.clone(),
                    column_name.clone(),
                    Vec::new(),
                ));
            }

            assert!(
                !column_statistics.is_empty()
                    && column_statistics.last().unwrap().column_id == column_id,
                "Column statistics should not be empty and last column_id should match current column_id"
            );

            if let Some(last_column_stat) = column_statistics.last_mut() {
                if let (Some(st), Some(pl)) = (stats_type, payload) {
                    let data = serde_json::from_str(&pl).unwrap_or(Value::Null);
                    last_column_stat.add_advanced_stat(AdvanceColumnStatistics {
                        stats_type: st,
                        data,
                    });
                }
            }

            // Assuming all columns have the same record_count, only need to set once
            if !row_flag {
                row_count = record_count as usize;
                row_flag = true;
            }
        }

        TableStatistics {
            row_count,
            column_statistics,
        }
    }
}

/// Statistics for a single column including type, name, and advanced statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub column_id: i64,
    pub column_type: String,
    pub name: String,
    pub advanced_stats: Vec<AdvanceColumnStatistics>,
}

impl ColumnStatistics {
    fn new(
        column_id: i64,
        column_type: String,
        name: String,
        advanced_stats: Vec<AdvanceColumnStatistics>,
    ) -> Self {
        Self {
            column_id,
            column_type,
            name,
            advanced_stats,
        }
    }

    fn add_advanced_stat(&mut self, stat: AdvanceColumnStatistics) {
        self.advanced_stats.push(stat);
    }
}

/// An advanced statistics entry with type and serialized data at a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceColumnStatistics {
    pub stats_type: String,
    pub data: Value,
}

/// Identifier for a snapshot in the statistics database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotId(pub i64);

/// Snapshot metadata including schema version and next IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub schema_version: i64,
    pub next_catalog_id: i64,
    pub next_file_id: i64,
}

/// Schema information including name, ID, and valid snapshot range.
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct CurrentSchema {
    pub schema_name: String,
    pub schema_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StatisticsUpdate {
    stats_type: String,
    payload: String,
}

/// Trait defining operations for managing table statistics with snapshot-based time travel.
pub trait StatisticsProvider {
    /// Fetches the current (most recent) snapshot ID.
    fn fetch_current_snapshot(&self) -> Result<SnapshotId, Error>;

    /// Fetches complete metadata for the current snapshot.
    fn fetch_current_snapshot_info(&self) -> Result<SnapshotInfo, Error>;

    /// Fetches the Arrow schema for a table at the current snapshot.
    fn fetch_current_schema(&self, schema: Option<&str>, table: &str) -> Result<SchemaRef, Error>;

    /// Fetches schema information including name, ID, and snapshot range.
    fn fetch_current_schema_info(&self) -> Result<CurrentSchema, Error>;

    /// Retrieves table and column statistics at a specific snapshot.
    fn fetch_table_statistics(
        &self,
        table_name: &str,
        snapshot: i64,
        connection: &Connection,
    ) -> Result<Option<TableStatistics>, Error>;

    /// Updates or inserts advanced statistics for a table column.
    fn update_table_column_stats(
        &self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error>;
}

/// DuckLake-based implementation of StatisticsProvider using DuckDB with snapshot management.
pub struct DuckLakeStatisticsProvider {
    conn: Connection,
}

impl DuckLakeStatisticsProvider {
    /// Converts a DuckDB type string to an Arrow DataType.
    fn duckdb_type_to_arrow(type_str: &str) -> Result<DataType, Error> {
        // Handle common DuckDB types
        let data_type = match type_str.to_uppercase().as_str() {
            "INTEGER" | "INT" | "INT4" => DataType::Int32,
            "BIGINT" | "INT8" | "LONG" => DataType::Int64,
            "SMALLINT" | "INT2" | "SHORT" => DataType::Int16,
            "TINYINT" | "INT1" => DataType::Int8,
            "DOUBLE" | "FLOAT8" => DataType::Float64,
            "FLOAT" | "REAL" | "FLOAT4" => DataType::Float32,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "VARCHAR" | "TEXT" | "STRING" => DataType::Utf8,
            "DATE" => DataType::Date32,
            "TIMESTAMP" => {
                DataType::Timestamp(duckdb::arrow::datatypes::TimeUnit::Microsecond, None)
            }
            "TIME" => DataType::Time64(duckdb::arrow::datatypes::TimeUnit::Microsecond),
            "BLOB" | "BYTEA" | "BINARY" => DataType::Binary,
            "DECIMAL" => DataType::Decimal128(38, 10), // Default precision and scale
            _ => {
                // For unsupported types, use Utf8 as fallback or you could error out
                // Here we'll just return an error through the ArrowDataTypeConversion variant
                return Err(Error::ArrowDataTypeConversion {
                    source: DuckDBError::FromSqlConversionFailure(
                        0,
                        duckdb::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Unsupported DuckDB type for Arrow conversion: {}", type_str),
                        )),
                    ),
                });
            }
        };
        Ok(data_type)
    }

    /// Creates a new DuckLakeStatisticsProvider with optional file paths.
    /// If `location` is None, uses in-memory database. If `metadata_path` is None, uses default metadata file.
    pub fn try_new(location: Option<&str>, metadata_path: Option<&str>) -> Result<Self, Error> {
        let conn = if let Some(path) = location {
            Connection::open(path).context(ConnectionSnafu)?
        } else {
            Connection::open_in_memory().context(ConnectionSnafu)?
        };

        // Use provided metadata path or default to DEFAULT_METADATA_FILE
        let metadata_file = metadata_path.unwrap_or(DEFAULT_METADATA_FILE);
        let setup_query = format!(
            r#"
            INSTALL ducklake;
            LOAD ducklake;
            ATTACH 'ducklake:{}' AS metalake;
            USE metalake;

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats (
                column_id BIGINT,
                begin_snapshot BIGINT,
                end_snapshot BIGINT,
                table_id BIGINT,
                stats_type VARCHAR,
                payload VARCHAR
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_query (
                query_id BIGINT,
                query_string VARCHAR,
                root_group_id BIGINT
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_query_instance (
                query_instance_id BIGINT PRIMARY KEY,
                query_id BIGINT,
                creation_time BIGINT,
                snapshot_id BIGINT
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_group (
                group_id BIGINT,
                begin_snapshot BIGINT,
                end_snapshot BIGINT
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_group_stats (
                group_id BIGINT,
                begin_snapshot BIGINT,
                end_snapshot BIGINT,
                stats_type VARCHAR,
                payload VARCHAR
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_execution_subplan_feedback (
                group_id BIGINT,
                begin_snapshot BIGINT,
                end_snapshot BIGINT,
                stats_type VARCHAR,
                payload VARCHAR
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_subplan_scalar_feedback (
                scalar_id BIGINT,
                group_id BIGINT,
                stats_type VARCHAR,
                payload VARCHAR,
                query_instance_id BIGINT
            );
        "#,
            metadata_file
        );
        conn.execute_batch(&setup_query).context(ConnectionSnafu)?;
        Ok(Self { conn })
    }

    /// Returns a reference to the underlying DuckDB connection.
    pub fn get_connection(&self) -> &Connection {
        &self.conn
    }

    /// Begins a database transaction.
    fn begin_transaction(&self) -> Result<(), Error> {
        self.conn
            .execute_batch("BEGIN TRANSACTION;")
            .context(QueryExecutionSnafu)
    }

    /// Commits the current database transaction.
    fn commit_transaction(&self) -> Result<(), Error> {
        self.conn
            .execute_batch("COMMIT TRANSACTION;")
            .context(QueryExecutionSnafu)
    }
}

impl StatisticsProvider for DuckLakeStatisticsProvider {
    fn fetch_current_snapshot(&self) -> Result<SnapshotId, Error> {
        self.conn
            .prepare("FROM ducklake_current_snapshot('metalake');")
            .context(QueryExecutionSnafu)?
            .query_row([], |row| Ok(SnapshotId(row.get(0)?)))
            .context(QueryExecutionSnafu)
    }

    fn fetch_current_snapshot_info(&self) -> Result<SnapshotInfo, Error> {
        self.conn
            .prepare(
                r#"
                SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
                    FROM __ducklake_metadata_metalake.main.ducklake_snapshot
                    WHERE snapshot_id = (SELECT MAX(snapshot_id)
                        FROM __ducklake_metadata_metalake.main.ducklake_snapshot);
                "#,
            )
            .context(QueryExecutionSnafu)?
            .query_row([], |row| {
                Ok(SnapshotInfo {
                    snapshot_id: row.get("snapshot_id")?,
                    schema_version: row.get("schema_version")?,
                    next_catalog_id: row.get("next_catalog_id")?,
                    next_file_id: row.get("next_file_id")?,
                })
            })
            .context(QueryExecutionSnafu)
    }

    fn fetch_current_schema(&self, schema: Option<&str>, table: &str) -> Result<SchemaRef, Error> {
        let table_ref = schema
            .map(|s| format!("{}.{}", s, table))
            .unwrap_or_else(|| table.to_string());

        let schema_query = format!("DESCRIBE {};", table_ref);

        let mut stmt = self
            .conn
            .prepare(&schema_query)
            .context(QueryExecutionSnafu)?;

        let mut fields = Vec::new();
        let column_iter = stmt
            .query_map([], |row| {
                let column_name: String = row.get("column_name")?;
                let column_type_str: String = row.get("column_type")?;
                let null: String = row.get("null")?;

                // Convert DuckDB type to Arrow type
                let column_type = Self::duckdb_type_to_arrow(&column_type_str).map_err(|_| {
                    DuckDBError::FromSqlConversionFailure(
                        0,
                        duckdb::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Could not convert DuckDB type '{}' to Arrow type",
                                column_type_str
                            ),
                        )),
                    )
                })?;

                fields.push(Field::new(column_name, column_type, null == "YES"));
                Ok(())
            })
            .context(QueryExecutionSnafu)?;

        for result in column_iter {
            result.context(QueryExecutionSnafu)?;
        }
        let schema = Schema::new(fields);
        Ok(Arc::new(schema))
    }

    fn fetch_current_schema_info(&self) -> Result<CurrentSchema, Error> {
        self.conn
            .prepare(
                r#"
                SELECT ds.schema_id, ds.schema_name, ds.begin_snapshot, ds.end_snapshot
                    FROM __ducklake_metadata_metalake.main.ducklake_schema ds
                    WHERE ds.schema_name = current_schema();
                "#,
            )
            .context(QueryExecutionSnafu)?
            .query_row([], |row| {
                Ok(CurrentSchema {
                    schema_name: row.get("schema_name")?,
                    schema_id: row.get("schema_id")?,
                    begin_snapshot: row.get("begin_snapshot")?,
                    end_snapshot: row.get("end_snapshot")?,
                })
            })
            .context(QueryExecutionSnafu)
    }

    fn fetch_table_statistics(
        &self,
        table: &str,
        snapshot: i64,
        conn: &Connection,
    ) -> Result<Option<TableStatistics>, Error> {
        let mut stmt = conn
            .prepare(FETCH_TABLE_STATS_QUERY)
            .context(QueryExecutionSnafu)?;

        let entries = stmt
            .query_map(
                [
                    &snapshot.to_string(),
                    &snapshot.to_string(),
                    table,
                    &snapshot.to_string(),
                    &snapshot.to_string(),
                ],
                |row| {
                    Ok(TableColumnStatisticsEntry {
                        table_id: row.get("table_id")?,
                        column_id: row.get("column_id")?,
                        column_name: row.get("column_name")?,
                        column_type: row.get("column_type")?,
                        record_count: row.get("record_count")?,
                        next_row_id: row.get("next_row_id")?,
                        file_size_bytes: row.get("file_size_bytes")?,
                        stats_type: row.get("stats_type")?,
                        payload: row.get("payload")?,
                    })
                },
            )
            .context(QueryExecutionSnafu)?
            .map(|result| result.context(QueryExecutionSnafu));

        Ok(Some(TableStatistics::from_iter(entries)))
    }

    /// Update table column statistics
    fn update_table_column_stats(
        &self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        // Start transaction
        self.begin_transaction()?;

        // Fetch current snapshot info
        let current_snapshot = self.fetch_current_snapshot_info()?;
        let current_snapshot_id = current_snapshot.snapshot_id;

        // Update matching past snapshot to close it
        self.conn
            .prepare(UPDATE_ADV_STATS_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                current_snapshot_id + 1,
                stats_type,
                column_id,
                table_id,
            ])
            .context(QueryExecutionSnafu)?;

        // Insert new snapshot
        self.conn
            .prepare(INSERT_ADV_STATS_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                column_id,
                current_snapshot_id + 1,
                Null,
                table_id,
                stats_type,
                payload,
            ])
            .context(QueryExecutionSnafu)?;

        self.conn
            .prepare(INSERT_SNAPSHOT_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                current_snapshot_id + 1,
                current_snapshot.schema_version,
                current_snapshot.next_catalog_id,
                current_snapshot.next_file_id,
            ])
            .context(QueryExecutionSnafu)?;

        self.conn
            .prepare(INSERT_SNAPSHOT_CHANGE_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                current_snapshot_id + 1,
                format!(
                    r#"updated_stats:"main"."ducklake_table_column_adv_stats",{}:{}"#,
                    stats_type, payload
                ),
                Null,
                Null,
                Null,
            ])
            .context(QueryExecutionSnafu)?;

        // Commit transaction
        self.commit_transaction()?;

        Ok(())
    }
}
