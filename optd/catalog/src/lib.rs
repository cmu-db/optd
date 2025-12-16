use duckdb::{
    Connection, Error as DuckDBError,
    arrow::datatypes::{Field, Schema, SchemaRef},
    params,
    types::Null,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::{ResultExt, prelude::*};
use std::{collections::HashMap, sync::Arc};

mod service;
pub use service::{CatalogBackend, CatalogRequest, CatalogService, CatalogServiceHandle};

/// Operations for managing table statistics with snapshot-based time travel.
pub trait Catalog {
    /// Gets the current (most recent) snapshot ID.
    fn current_snapshot(&mut self) -> Result<SnapshotId, Error>;

    /// Gets complete metadata for the current snapshot.
    fn current_snapshot_info(&mut self) -> Result<SnapshotInfo, Error>;

    /// Gets the Arrow schema for a table at the current snapshot.
    fn current_schema(&mut self, schema: Option<&str>, table: &str) -> Result<SchemaRef, Error>;

    /// Gets schema information including name, ID, and snapshot range.
    fn current_schema_info(&mut self) -> Result<CurrentSchema, Error>;

    /// Retrieves table and column statistics at a specific snapshot.
    fn table_statistics(
        &mut self,
        table_name: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<TableStatistics>, Error>;

    /// Updates or inserts advanced statistics for a table column.
    fn update_table_column_stats(
        &mut self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error>;

    /// Registers a new external table in the catalog.
    fn register_external_table(
        &mut self,
        request: RegisterTableRequest,
    ) -> Result<ExternalTableMetadata, Error>;

    /// Retrieves external table metadata by name.
    fn get_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Option<ExternalTableMetadata>, Error>;

    /// Lists all active external tables in a schema.
    fn list_external_tables(
        &mut self,
        schema_name: Option<&str>,
    ) -> Result<Vec<ExternalTableMetadata>, Error>;

    /// Soft-deletes an external table by setting its end_snapshot.
    fn drop_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<(), Error>;

    /// Retrieves external table metadata at a specific snapshot (time-travel).
    fn get_external_table_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ExternalTableMetadata>, Error>;

    /// Lists all external tables active at a specific snapshot (time-travel).
    fn list_external_tables_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        snapshot_id: i64,
    ) -> Result<Vec<ExternalTableMetadata>, Error>;

    /// Lists all snapshots with their metadata.
    fn list_snapshots(&mut self) -> Result<Vec<SnapshotInfo>, Error>;

    /// Sets statistics for any table.
    /// Works for both internal DuckDB tables and external tables.
    fn set_table_statistics(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), Error>;
}

const DEFAULT_METADATA_FILE: &str = "metadata.ducklake";

const CREATE_EXTRA_TABLES_QUERY: &str = r#"
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

    CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_external_table (
        table_id BIGINT PRIMARY KEY,
        schema_id BIGINT NOT NULL,
        table_name VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        file_format VARCHAR NOT NULL,
        compression VARCHAR,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_external_table_options (
        table_id BIGINT NOT NULL,
        option_key VARCHAR NOT NULL,
        option_value VARCHAR NOT NULL,
        PRIMARY KEY (table_id, option_key)
    );

    CREATE INDEX IF NOT EXISTS idx_optd_external_table_schema 
        ON __ducklake_metadata_metalake.main.optd_external_table(schema_id, table_name, end_snapshot);

    CREATE INDEX IF NOT EXISTS idx_optd_external_table_snapshot
        ON __ducklake_metadata_metalake.main.optd_external_table(begin_snapshot, end_snapshot);
"#;

// SQL query to fetch the latest snapshot information.
const SNAPSHOT_INFO_QUERY: &str = r#"
    SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
    FROM __ducklake_metadata_metalake.main.ducklake_snapshot
    WHERE snapshot_id = (SELECT MAX(snapshot_id)
        FROM __ducklake_metadata_metalake.main.ducklake_snapshot);
"#;

// SQL query to fetch schema information including name, ID, and snapshot valid range.
const SCHEMA_INFO_QUERY: &str = r#"
    SELECT ds.schema_id, ds.schema_name, ds.begin_snapshot, ds.end_snapshot
        FROM __ducklake_metadata_metalake.main.ducklake_schema ds
        WHERE ds.schema_name = current_schema();
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
    #[snafu(display("Transaction error: {}", source))]
    Transaction { source: DuckDBError },
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
    #[snafu(display("Table '{}' does not exist", table_name))]
    TableNotFound { table_name: String },
}

/// Internal representation of a row from the table statistics query.
/// Used for collecting data before aggregating into TableStatistics.
struct TableColumnStatisticsEntry {
    _table_id: i64,
    column_id: i64,
    column_name: String,
    column_type: String,
    record_count: i64,
    _next_row_id: i64,
    _file_size_bytes: i64,
    stats_type: Option<String>,
    payload: Option<String>,
}

/// Statistics for a table including row count and per-column statistics.
/// Used for both reading and writing statistics (internal and external tables).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub row_count: usize,
    pub column_statistics: Vec<ColumnStatistics>,

    /// Total size of the table file(s) in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<usize>,
}

impl FromIterator<Result<TableColumnStatisticsEntry, Error>> for TableStatistics {
    fn from_iter<T: IntoIterator<Item = Result<TableColumnStatisticsEntry, Error>>>(
        iter: T,
    ) -> Self {
        let mut row_flag = false;
        let mut row_count = 0;
        let mut column_statistics = Vec::new();

        // Stats will be ordered by table_id then column_id
        for e in iter.into_iter().flatten() {
            // Check if unique table/column combination
            if column_statistics
                .last()
                .is_none_or(|last: &ColumnStatistics| last.column_id != e.column_id)
            {
                // New column encountered
                column_statistics.push(ColumnStatistics::new(
                    e.column_id,
                    e.column_type.clone(),
                    e.column_name.clone(),
                    Vec::new(),
                ));
            }

            assert!(
                !column_statistics.is_empty()
                    && column_statistics.last().unwrap().column_id == e.column_id,
                "Column statistics should not be empty and last column_id should match current column_id"
            );

            if let Some(last_column_stat) = column_statistics.last_mut()
                && let (Some(stats_type), Some(payload)) = (e.stats_type, e.payload)
            {
                let data = serde_json::from_str(&payload).unwrap_or(Value::Null);
                last_column_stat.add_advanced_stat(AdvanceColumnStatistics { stats_type, data });
            }

            // Assuming all columns have the same record_count, only need to set once
            if !row_flag {
                row_count = e.record_count as usize;
                row_flag = true;
            }
        }

        TableStatistics {
            row_count,
            column_statistics,
            size_bytes: None, // Not populated from database queries
        }
    }
}

/// Statistics for a single column including type, name, and advanced statistics.
/// For external tables without ducklake_column entries, column_id will be 0 and name identifies the column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub column_id: i64,
    pub column_type: String,
    pub name: String,
    pub advanced_stats: Vec<AdvanceColumnStatistics>,

    /// Minimum value in the column (serialized as JSON string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_value: Option<String>,
    /// Maximum value in the column (serialized as JSON string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value: Option<String>,
    /// Total number of null values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_count: Option<usize>,
    /// Number of distinct values (NDV)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<usize>,
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
            min_value: None,
            max_value: None,
            null_count: None,
            distinct_count: None,
        }
    }

    fn add_advanced_stat(&mut self, stat: AdvanceColumnStatistics) {
        self.advanced_stats.push(stat);
    }
}

/// An advanced statistics entry with type and serialized data at a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceColumnStatistics {
    /// Type of the statistical summaries (e.g., histogram, distinct count).
    pub stats_type: String,
    /// Serialized data for the statistics at a snapshot.
    pub data: Value,
}

/// Identifier for a snapshot in the statistics database.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SnapshotId(pub i64);

/// Snapshot metadata including schema version and next IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct SnapshotInfo {
    pub id: SnapshotId,
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

/// Metadata for an external table including location, format, and options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalTableMetadata {
    pub table_id: i64,
    pub schema_id: i64,
    pub table_name: String,
    pub location: String,
    pub file_format: String,
    pub compression: Option<String>,
    pub options: HashMap<String, String>,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
}

// ExternalTableStatistics and ExternalColumnStatistics removed - use unified TableStatistics and AdvanceColumnStatistics instead

/// Request to register a new external table in the catalog.
#[derive(Debug, Clone)]
pub struct RegisterTableRequest {
    pub table_name: String,
    pub schema_name: Option<String>,
    pub location: String,
    pub file_format: String,
    pub compression: Option<String>,
    pub options: HashMap<String, String>,
}

/// A catalog implementation using DuckDB with snapshot management.
pub struct DuckLakeCatalog {
    conn: Connection,
}

impl Catalog for DuckLakeCatalog {
    fn current_snapshot(&mut self) -> Result<SnapshotId, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::current_snapshot_inner(&txn);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn current_snapshot_info(&mut self) -> Result<SnapshotInfo, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::current_snapshot_info_inner(&txn);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn current_schema(&mut self, schema: Option<&str>, table: &str) -> Result<SchemaRef, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::current_schema_inner(&txn, schema, table);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn current_schema_info(&mut self) -> Result<CurrentSchema, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::current_schema_info_inner(&txn);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn table_statistics(
        &mut self,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<TableStatistics>, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::table_statistics_inner(&txn, None, table, Some(snapshot));
        txn.commit().context(TransactionSnafu)?;
        result
    }

    /// Update table column statistics
    fn update_table_column_stats(
        &mut self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result =
            Self::update_table_column_stats_inner(&txn, column_id, table_id, stats_type, payload);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn register_external_table(
        &mut self,
        request: RegisterTableRequest,
    ) -> Result<ExternalTableMetadata, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::register_external_table_inner(&txn, request);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn get_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::get_external_table_inner(&txn, schema_name, table_name, None);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn list_external_tables(
        &mut self,
        schema_name: Option<&str>,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::list_external_tables_inner(&txn, schema_name, None);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn drop_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<(), Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::drop_external_table_inner(&txn, schema_name, table_name);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn get_external_table_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result =
            Self::get_external_table_at_snapshot_inner(&txn, schema_name, table_name, snapshot_id);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn list_external_tables_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        snapshot_id: i64,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::list_external_tables_at_snapshot_inner(&txn, schema_name, snapshot_id);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn list_snapshots(&mut self) -> Result<Vec<SnapshotInfo>, Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::list_snapshots_inner(&txn);
        txn.commit().context(TransactionSnafu)?;
        result
    }

    fn set_table_statistics(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), Error> {
        let txn = self.conn.transaction().context(TransactionSnafu)?;
        let result = Self::set_table_statistics_inner(&txn, schema_name, table_name, stats);
        txn.commit().context(TransactionSnafu)?;
        result
    }
}

impl DuckLakeCatalog {
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
            ATTACH 'ducklake:{metadata_file}' AS metalake;
            USE metalake;

            {CREATE_EXTRA_TABLES_QUERY}
        "#
        );
        conn.execute_batch(&setup_query).context(ConnectionSnafu)?;
        Ok(Self { conn })
    }

    /// Returns a reference to the underlying DuckDB connection.
    pub fn get_connection(&self) -> &Connection {
        &self.conn
    }

    fn current_snapshot_inner(conn: &Connection) -> Result<SnapshotId, Error> {
        conn.prepare("FROM ducklake_current_snapshot('metalake');")
            .context(QueryExecutionSnafu)?
            .query_row([], |row| Ok(SnapshotId(row.get(0)?)))
            .context(QueryExecutionSnafu)
    }

    fn current_snapshot_info_inner(conn: &Connection) -> Result<SnapshotInfo, Error> {
        conn.prepare(SNAPSHOT_INFO_QUERY)
            .context(QueryExecutionSnafu)?
            .query_row([], |row| {
                Ok(SnapshotInfo {
                    id: SnapshotId(row.get("snapshot_id")?),
                    schema_version: row.get("schema_version")?,
                    next_catalog_id: row.get("next_catalog_id")?,
                    next_file_id: row.get("next_file_id")?,
                })
            })
            .context(QueryExecutionSnafu)
    }

    fn current_schema_inner(
        conn: &Connection,
        schema: Option<&str>,
        table: &str,
    ) -> Result<SchemaRef, Error> {
        let table_ref = schema
            .map(|s| format!("{}.{}", s, table))
            .unwrap_or_else(|| table.to_string());

        // Use SELECT * with LIMIT 0 to get schema with data types
        let schema_query = format!("SELECT * FROM {table_ref} LIMIT 0;");
        let mut stmt = conn.prepare(&schema_query).context(QueryExecutionSnafu)?;
        let arrow_result = stmt.query_arrow([]).context(QueryExecutionSnafu)?;
        let arrow_schema = arrow_result.get_schema();

        // Get nullable info from DESCRIBE
        // This is to fix Arrow API limitation with nullable info
        let describe_query = format!("DESCRIBE {table_ref}");
        let mut stmt = conn.prepare(&describe_query).context(QueryExecutionSnafu)?;
        let mut nullable_map = HashMap::new();
        let mut rows = stmt.query([]).context(QueryExecutionSnafu)?;

        while let Some(row) = rows.next().context(QueryExecutionSnafu)? {
            let col_name: String = row.get(0).context(QueryExecutionSnafu)?;
            let null_str: String = row.get(2).context(QueryExecutionSnafu)?;
            nullable_map.insert(col_name, null_str == "YES");
        }

        // Rebuild schema with correct nullable flags
        let fields: Vec<_> = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                let nullable = nullable_map
                    .get(field.name().as_str())
                    .copied()
                    .unwrap_or(true);
                Arc::new(Field::new(
                    field.name().as_str(),
                    field.data_type().clone(),
                    nullable,
                ))
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    fn current_schema_info_inner(conn: &Connection) -> Result<CurrentSchema, Error> {
        conn.prepare(SCHEMA_INFO_QUERY)
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

    /// Fetch table statistics using the FromIterator pattern for optimizer compatibility.
    ///
    /// Unified implementation that handles both internal tables (with ducklake_column)
    /// and external tables (with column_id=0 and names in JSON payloads).
    ///
    /// # Parameters
    /// - `schema_name`: Optional schema name (None = current schema)
    /// - `table`: Table name
    /// - `snapshot`: Optional snapshot ID (None = current snapshot)
    ///
    /// Returns None if table doesn't exist or has no statistics.
    fn table_statistics_inner(
        conn: &Connection,
        schema_name: Option<&str>,
        table: &str,
        snapshot: Option<SnapshotId>,
    ) -> Result<Option<TableStatistics>, Error> {
        let schema_info = Self::current_schema_info_inner(conn)?;
        let query_snapshot = match snapshot {
            Some(snap) => snap,
            None => Self::current_snapshot_info_inner(conn)?.id,
        };

        // Step 1: Get table_id and check if it's an internal table
        let table_lookup: Result<(i64, bool), _> = conn
            .prepare(
                r#"
                SELECT table_id, 1 as is_internal FROM __ducklake_metadata_metalake.main.ducklake_table
                WHERE schema_id = ? AND table_name = ?
                UNION ALL
                SELECT table_id, 0 as is_internal FROM __ducklake_metadata_metalake.main.optd_external_table
                WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL
                LIMIT 1
                "#,
            )
            .context(QueryExecutionSnafu)?
            .query_row(
                params![
                    schema_info.schema_id,
                    table,
                    schema_info.schema_id,
                    table
                ],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? == 1)),
            );

        let (table_id, is_internal_table) = match table_lookup {
            Ok(result) => result,
            Err(DuckDBError::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(Error::QueryExecution { source: e }),
        };

        // Step 2: Fetch row count and file size (may not exist for tables without statistics)
        let stats_result: Result<(i64, Option<i64>), _> = conn
            .prepare(
                r#"
                SELECT record_count, file_size_bytes
                FROM __ducklake_metadata_metalake.main.ducklake_table_stats
                WHERE table_id = ? AND record_count IS NOT NULL
                "#,
            )
            .context(QueryExecutionSnafu)?
            .query_row(params![table_id], |row| Ok((row.get(0)?, row.get(1)?)));

        let (record_count, file_size_bytes) = match stats_result {
            Ok((count, size)) => (count, size),
            Err(DuckDBError::QueryReturnedNoRows) => {
                // No statistics exist yet
                // For internal tables, we should still return column metadata
                if is_internal_table {
                    // Query ducklake_column to get column metadata
                    let mut stmt = conn
                        .prepare(
                            r#"
                            SELECT column_id, column_name, column_type
                            FROM __ducklake_metadata_metalake.main.ducklake_column
                            WHERE table_id = ?
                            ORDER BY column_id
                            "#,
                        )
                        .context(QueryExecutionSnafu)?;

                    let columns = stmt
                        .query_map(params![table_id], |row| {
                            Ok(ColumnStatistics {
                                column_id: row.get(0)?,
                                column_type: row.get(2)?,
                                name: row.get(1)?,
                                advanced_stats: Vec::new(),
                                min_value: None,
                                max_value: None,
                                null_count: None,
                                distinct_count: None,
                            })
                        })
                        .context(QueryExecutionSnafu)?
                        .collect::<Result<Vec<_>, _>>()
                        .context(QueryExecutionSnafu)?;

                    return Ok(Some(TableStatistics {
                        row_count: 0,
                        column_statistics: columns,
                        size_bytes: None,
                    }));
                } else {
                    // External table without statistics - return None
                    return Ok(None);
                }
            }
            Err(e) => return Err(Error::QueryExecution { source: e }),
        };

        // Step 3: Fetch column statistics from ducklake_table_column_adv_stats
        let mut stmt = conn
            .prepare(
                r#"
                SELECT column_id, stats_type, payload
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = ? 
                  AND ? >= begin_snapshot 
                  AND (? < end_snapshot OR end_snapshot IS NULL)
                ORDER BY column_id, stats_type
                "#,
            )
            .context(QueryExecutionSnafu)?;

        let rows = stmt
            .query_map(
                params![table_id, query_snapshot.0, query_snapshot.0],
                |row| {
                    let column_id: i64 = row.get(0)?;
                    let stats_type: String = row.get(1)?;
                    let payload: String = row.get(2)?;
                    Ok((column_id, stats_type, payload))
                },
            )
            .context(QueryExecutionSnafu)?;

        // Step 4: Build TableColumnStatisticsEntry objects for FromIterator
        let mut entries: Vec<TableColumnStatisticsEntry> = Vec::new();
        let mut column_data: HashMap<i64, (String, String)> = HashMap::new(); // column_id -> (name, type)
        let mut external_column_mapping: HashMap<String, i64> = HashMap::new(); // column_name -> unique negative ID for external tables
        let mut next_external_id = -1i64;

        for row_result in rows {
            let (column_id, stats_type, payload) = row_result.context(QueryExecutionSnafu)?;
            let mut parsed: serde_json::Value =
                serde_json::from_str(&payload).context(JsonSerializationSnafu)?;

            // Resolve column_name and assign unique column_id for external tables
            let (effective_column_id, column_name) = if column_id == 0 {
                // External table: extract column_name from JSON payload
                let name = parsed["column_name"].as_str().unwrap_or("").to_string();
                // Remove column_name from payload for cleaner advanced_stats
                if let Value::Object(ref mut map) = parsed {
                    map.remove("column_name");
                }

                // Assign unique negative column_id for this external column
                let effective_id =
                    *external_column_mapping
                        .entry(name.clone())
                        .or_insert_with(|| {
                            let id = next_external_id;
                            next_external_id -= 1;
                            id
                        });

                (effective_id, name)
            } else {
                // Internal table: query ducklake_column if we haven't already
                let name = if let Some((name, _)) = column_data.get(&column_id) {
                    name.clone()
                } else {
                    let name: String = conn
                        .query_row(
                            "SELECT column_name, column_type FROM __ducklake_metadata_metalake.main.ducklake_column
                             WHERE column_id = ? AND table_id = ?",
                            params![column_id, table_id],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                        )
                        .map(|(n, t)| {
                            column_data.insert(column_id, (n.clone(), t));
                            n
                        })
                        .unwrap_or_else(|_| format!("column_{}", column_id));
                    name
                };
                (column_id, name)
            };

            // Get column_type (empty string for external tables, will be populated later if needed)
            let column_type = column_data
                .get(&effective_column_id)
                .map(|(_, t)| t.clone())
                .unwrap_or_default();

            // Create entry for this stat using effective_column_id
            entries.push(TableColumnStatisticsEntry {
                _table_id: table_id,
                column_id: effective_column_id, // Use negative IDs for external tables
                column_name,
                column_type,
                record_count,
                _next_row_id: 0, // Not used in FromIterator
                _file_size_bytes: file_size_bytes.unwrap_or(0),
                stats_type: Some(stats_type),
                payload: Some(parsed.to_string()),
            });
        }

        // If no column stats, handle based on table type
        if entries.is_empty() {
            if is_internal_table {
                // For internal tables, return column metadata even without stats
                let mut stmt = conn
                    .prepare(
                        r#"
                        SELECT column_id, column_name, column_type
                        FROM __ducklake_metadata_metalake.main.ducklake_column
                        WHERE table_id = ?
                        ORDER BY column_id
                        "#,
                    )
                    .context(QueryExecutionSnafu)?;

                let columns = stmt
                    .query_map(params![table_id], |row| {
                        Ok(ColumnStatistics {
                            column_id: row.get(0)?,
                            column_type: row.get(2)?,
                            name: row.get(1)?,
                            advanced_stats: Vec::new(),
                            min_value: None,
                            max_value: None,
                            null_count: None,
                            distinct_count: None,
                        })
                    })
                    .context(QueryExecutionSnafu)?
                    .collect::<Result<Vec<_>, _>>()
                    .context(QueryExecutionSnafu)?;

                return Ok(Some(TableStatistics {
                    row_count: record_count as usize,
                    column_statistics: columns,
                    size_bytes: file_size_bytes.map(|s| s as usize),
                }));
            } else {
                // External table without column stats - just return row count
                return Ok(Some(TableStatistics {
                    row_count: record_count as usize,
                    column_statistics: Vec::new(),
                    size_bytes: file_size_bytes.map(|s| s as usize),
                }));
            }
        }

        // Convert entries to TableStatistics using FromIterator
        let mut result = TableStatistics::from_iter(entries.into_iter().map(Ok));

        // For internal tables, ensure ALL columns are included (even those without stats)
        if is_internal_table {
            // Query all columns from ducklake_column
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT column_id, column_name, column_type
                    FROM __ducklake_metadata_metalake.main.ducklake_column
                    WHERE table_id = ?
                    ORDER BY column_id
                    "#,
                )
                .context(QueryExecutionSnafu)?;

            let all_columns: Vec<(i64, String, String)> = stmt
                .query_map(params![table_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .context(QueryExecutionSnafu)?
                .collect::<Result<Vec<_>, _>>()
                .context(QueryExecutionSnafu)?;

            // Build a set of column_ids that already have statistics
            let existing_column_ids: std::collections::HashSet<i64> = result
                .column_statistics
                .iter()
                .map(|cs| cs.column_id)
                .collect();

            // Add columns that don't have statistics yet
            for (col_id, col_name, col_type) in all_columns {
                if !existing_column_ids.contains(&col_id) {
                    result.column_statistics.push(ColumnStatistics {
                        column_id: col_id,
                        column_type: col_type,
                        name: col_name,
                        advanced_stats: Vec::new(),
                        min_value: None,
                        max_value: None,
                        null_count: None,
                        distinct_count: None,
                    });
                }
            }

            // Sort by column_id to maintain consistent ordering
            result.column_statistics.sort_by_key(|cs| cs.column_id);
        }

        // Normalize external table column_ids back to 0 (they were negative for grouping)
        // Also extract basic_stats into direct fields
        for col_stat in &mut result.column_statistics {
            if col_stat.column_id < 0 {
                col_stat.column_id = 0;
            }

            // Extract basic_stats into direct fields if present
            if let Some(basic_stat) = col_stat
                .advanced_stats
                .iter()
                .find(|s| s.stats_type == "basic_stats")
            {
                if let Some(min_val) = basic_stat.data.get("min_value").and_then(|v| v.as_str()) {
                    col_stat.min_value = Some(min_val.to_string());
                }
                if let Some(max_val) = basic_stat.data.get("max_value").and_then(|v| v.as_str()) {
                    col_stat.max_value = Some(max_val.to_string());
                }
                if let Some(null_cnt) = basic_stat.data.get("null_count").and_then(|v| v.as_u64()) {
                    col_stat.null_count = Some(null_cnt as usize);
                }
                if let Some(distinct_cnt) = basic_stat
                    .data
                    .get("distinct_count")
                    .and_then(|v| v.as_u64())
                {
                    col_stat.distinct_count = Some(distinct_cnt as usize);
                }
            }
        }

        // Set size_bytes from the fetched value (FromIterator doesn't populate this)
        result.size_bytes = file_size_bytes.map(|s| s as usize);

        Ok(Some(result))
    }

    fn update_table_column_stats_inner(
        conn: &Connection,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        // Fetch current snapshot info
        let curr_snapshot = Self::current_snapshot_info_inner(conn)?;

        // Update matching past snapshot to close it
        conn.prepare(UPDATE_ADV_STATS_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                stats_type,
                column_id,
                table_id,
            ])
            .context(QueryExecutionSnafu)?;

        // Insert new snapshot
        conn.prepare(INSERT_ADV_STATS_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                column_id,
                curr_snapshot.id.0 + 1,
                Null,
                table_id,
                stats_type,
                payload,
            ])
            .context(QueryExecutionSnafu)?;

        conn.prepare(INSERT_SNAPSHOT_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                curr_snapshot.schema_version,
                curr_snapshot.next_catalog_id,
                curr_snapshot.next_file_id,
            ])
            .context(QueryExecutionSnafu)?;

        conn.prepare(INSERT_SNAPSHOT_CHANGE_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                format!(
                    r#"updated_stats:"main"."ducklake_table_column_adv_stats",{stats_type}:{payload}"#,
                ),
                Null,
                Null,
                Null,
            ])
            .context(QueryExecutionSnafu)?;

        Ok(())
    }

    fn register_external_table_inner(
        conn: &Connection,
        request: RegisterTableRequest,
    ) -> Result<ExternalTableMetadata, Error> {
        // Get current schema info
        let schema_info = Self::current_schema_info_inner(conn)?;
        let curr_snapshot = Self::current_snapshot_info_inner(conn)?;

        // Generate negative table_id to avoid collision with internal tables.
        // Internal tables use positive IDs (1, 2, 3, ...), external tables use negative (-1, -2, -3, ...).
        let table_id: i64 = conn
            .query_row(
                r#"
                SELECT COALESCE(MIN(table_id), 0) - 1
                FROM __ducklake_metadata_metalake.main.optd_external_table
                "#,
                [],
                |row| row.get(0),
            )
            .context(QueryExecutionSnafu)?;

        // Insert table metadata
        conn.prepare(
            r#"
            INSERT INTO __ducklake_metadata_metalake.main.optd_external_table
                (table_id, schema_id, table_name, location, file_format, compression, begin_snapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .context(QueryExecutionSnafu)?
        .execute(params![
            table_id,
            schema_info.schema_id,
            &request.table_name,
            &request.location,
            &request.file_format,
            request.compression.as_deref(),
            curr_snapshot.id.0 + 1,  // Use next snapshot since we'll create it
        ])
        .context(QueryExecutionSnafu)?;

        // Insert table options
        for (key, value) in &request.options {
            conn.prepare(
                r#"
                INSERT INTO __ducklake_metadata_metalake.main.optd_external_table_options
                    (table_id, option_key, option_value)
                VALUES (?, ?, ?)
                "#,
            )
            .context(QueryExecutionSnafu)?
            .execute(params![table_id, key, value])
            .context(QueryExecutionSnafu)?;
        }

        // Create new snapshot for this table registration
        conn.prepare(INSERT_SNAPSHOT_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                curr_snapshot.schema_version,
                curr_snapshot.next_catalog_id,
                curr_snapshot.next_file_id,
            ])
            .context(QueryExecutionSnafu)?;

        conn.prepare(INSERT_SNAPSHOT_CHANGE_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                format!(r#"created_table:"{}""#, request.table_name),
                Null,
                Null,
                Null,
            ])
            .context(QueryExecutionSnafu)?;

        Ok(ExternalTableMetadata {
            table_id,
            schema_id: schema_info.schema_id,
            table_name: request.table_name,
            location: request.location,
            file_format: request.file_format,
            compression: request.compression,
            options: request.options,
            begin_snapshot: curr_snapshot.id.0 + 1,
            end_snapshot: None,
        })
    }

    /// Unified method to get external table metadata.
    /// If `snapshot_id` is None, gets the current active table (end_snapshot IS NULL).
    /// If `snapshot_id` is Some, gets the table as it existed at that snapshot.
    fn get_external_table_inner(
        conn: &Connection,
        _schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: Option<i64>,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        // Get schema_id
        let schema_info = Self::current_schema_info_inner(conn)?;

        // Query and extract data based on snapshot parameter
        let row_data = match snapshot_id {
            None => {
                let mut stmt = conn
                    .prepare(
                        r#"
                        SELECT table_id, schema_id, table_name, location, file_format, 
                               compression, begin_snapshot, end_snapshot
                        FROM __ducklake_metadata_metalake.main.optd_external_table
                        WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL
                        "#,
                    )
                    .context(QueryExecutionSnafu)?;

                let mut rows = stmt
                    .query(params![schema_info.schema_id, table_name])
                    .context(QueryExecutionSnafu)?;

                if let Some(row) = rows.next().context(QueryExecutionSnafu)? {
                    Some((
                        row.get::<_, i64>(0).context(QueryExecutionSnafu)?,
                        row.get::<_, i64>(1).context(QueryExecutionSnafu)?,
                        row.get::<_, String>(2).context(QueryExecutionSnafu)?,
                        row.get::<_, String>(3).context(QueryExecutionSnafu)?,
                        row.get::<_, String>(4).context(QueryExecutionSnafu)?,
                        row.get::<_, Option<String>>(5)
                            .context(QueryExecutionSnafu)?,
                        row.get::<_, i64>(6).context(QueryExecutionSnafu)?,
                        row.get::<_, Option<i64>>(7).context(QueryExecutionSnafu)?,
                    ))
                } else {
                    None
                }
            }
            Some(snapshot) => {
                let mut stmt = conn
                    .prepare(
                        r#"
                        SELECT table_id, schema_id, table_name, location, file_format, 
                               compression, begin_snapshot, end_snapshot
                        FROM __ducklake_metadata_metalake.main.optd_external_table
                        WHERE schema_id = ? AND table_name = ? 
                          AND begin_snapshot <= ?
                          AND (end_snapshot IS NULL OR end_snapshot > ?)
                        "#,
                    )
                    .context(QueryExecutionSnafu)?;

                let mut rows = stmt
                    .query(params![
                        schema_info.schema_id,
                        table_name,
                        snapshot,
                        snapshot
                    ])
                    .context(QueryExecutionSnafu)?;

                if let Some(row) = rows.next().context(QueryExecutionSnafu)? {
                    Some((
                        row.get::<_, i64>(0).context(QueryExecutionSnafu)?,
                        row.get::<_, i64>(1).context(QueryExecutionSnafu)?,
                        row.get::<_, String>(2).context(QueryExecutionSnafu)?,
                        row.get::<_, String>(3).context(QueryExecutionSnafu)?,
                        row.get::<_, String>(4).context(QueryExecutionSnafu)?,
                        row.get::<_, Option<String>>(5)
                            .context(QueryExecutionSnafu)?,
                        row.get::<_, i64>(6).context(QueryExecutionSnafu)?,
                        row.get::<_, Option<i64>>(7).context(QueryExecutionSnafu)?,
                    ))
                } else {
                    None
                }
            }
        };

        if let Some((
            table_id,
            schema_id,
            table_name,
            location,
            file_format,
            compression,
            begin_snapshot,
            end_snapshot,
        )) = row_data
        {
            // Fetch options
            let mut options = HashMap::new();
            let mut opt_stmt = conn
                .prepare(
                    r#"
                    SELECT option_key, option_value
                    FROM __ducklake_metadata_metalake.main.optd_external_table_options
                    WHERE table_id = ?
                    "#,
                )
                .context(QueryExecutionSnafu)?;

            let opt_rows = opt_stmt
                .query(params![table_id])
                .context(QueryExecutionSnafu)?;

            for opt_row in opt_rows.mapped(|r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            {
                let (key, value) = opt_row.context(QueryExecutionSnafu)?;
                options.insert(key, value);
            }

            Ok(Some(ExternalTableMetadata {
                table_id,
                schema_id,
                table_name,
                location,
                file_format,
                compression,
                options,
                begin_snapshot,
                end_snapshot,
            }))
        } else {
            Ok(None)
        }
    }

    /// Unified method to list external tables.
    /// If `snapshot_id` is None, lists current active tables (end_snapshot IS NULL).
    /// If `snapshot_id` is Some, lists tables as they existed at that snapshot.
    fn list_external_tables_inner(
        conn: &Connection,
        _schema_name: Option<&str>,
        snapshot_id: Option<i64>,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        let schema_info = Self::current_schema_info_inner(conn)?;

        // Collect table data based on snapshot parameter
        let table_rows = match snapshot_id {
            None => {
                let mut stmt = conn
                    .prepare(
                        r#"
                        SELECT table_id, schema_id, table_name, location, file_format,
                               compression, begin_snapshot, end_snapshot
                        FROM __ducklake_metadata_metalake.main.optd_external_table
                        WHERE schema_id = ? AND end_snapshot IS NULL
                        ORDER BY table_name
                        "#,
                    )
                    .context(QueryExecutionSnafu)?;

                let rows = stmt
                    .query(params![schema_info.schema_id])
                    .context(QueryExecutionSnafu)?;

                rows.mapped(|r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, i64>(6)?,
                        r.get::<_, Option<i64>>(7)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .context(QueryExecutionSnafu)?
            }
            Some(snapshot) => {
                let mut stmt = conn
                    .prepare(
                        r#"
                        SELECT table_id, schema_id, table_name, location, file_format,
                               compression, begin_snapshot, end_snapshot
                        FROM __ducklake_metadata_metalake.main.optd_external_table
                        WHERE schema_id = ? 
                          AND begin_snapshot <= ?
                          AND (end_snapshot IS NULL OR end_snapshot > ?)
                        ORDER BY table_name
                        "#,
                    )
                    .context(QueryExecutionSnafu)?;

                let rows = stmt
                    .query(params![schema_info.schema_id, snapshot, snapshot])
                    .context(QueryExecutionSnafu)?;

                rows.mapped(|r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, i64>(6)?,
                        r.get::<_, Option<i64>>(7)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .context(QueryExecutionSnafu)?
            }
        };

        // Now build ExternalTableMetadata for each table
        let mut tables = Vec::new();
        for (
            table_id,
            schema_id,
            table_name,
            location,
            file_format,
            compression,
            begin_snapshot,
            end_snapshot,
        ) in table_rows
        {
            // Fetch options for this table
            let mut options = HashMap::new();
            let mut opt_stmt = conn
                .prepare(
                    r#"
                    SELECT option_key, option_value
                    FROM __ducklake_metadata_metalake.main.optd_external_table_options
                    WHERE table_id = ?
                    "#,
                )
                .context(QueryExecutionSnafu)?;

            let opt_rows = opt_stmt
                .query(params![table_id])
                .context(QueryExecutionSnafu)?;

            for opt_row in opt_rows.mapped(|r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            {
                let (key, value) = opt_row.context(QueryExecutionSnafu)?;
                options.insert(key, value);
            }

            tables.push(ExternalTableMetadata {
                table_id,
                schema_id,
                table_name,
                location,
                file_format,
                compression,
                options,
                begin_snapshot,
                end_snapshot,
            });
        }

        Ok(tables)
    }

    fn list_snapshots_inner(conn: &Connection) -> Result<Vec<SnapshotInfo>, Error> {
        let query = "
            SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
            FROM __ducklake_metadata_metalake.main.ducklake_snapshot
            ORDER BY snapshot_id
        ";

        let mut stmt = conn.prepare(query).context(QueryExecutionSnafu)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(SnapshotInfo {
                    id: SnapshotId(row.get(0)?),
                    schema_version: row.get(1)?,
                    next_catalog_id: row.get(2)?,
                    next_file_id: row.get(3)?,
                })
            })
            .context(QueryExecutionSnafu)?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row.context(QueryExecutionSnafu)?);
        }

        Ok(snapshots)
    }

    fn drop_external_table_inner(
        conn: &Connection,
        _schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<(), Error> {
        let schema_info = Self::current_schema_info_inner(conn)?;
        let curr_snapshot = Self::current_snapshot_info_inner(conn)?;

        // Soft delete by setting end_snapshot
        let updated = conn
            .prepare(
                r#"
                UPDATE __ducklake_metadata_metalake.main.optd_external_table
                SET end_snapshot = ?
                WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL
                "#,
            )
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                schema_info.schema_id,
                table_name
            ])
            .context(QueryExecutionSnafu)?;

        if updated == 0 {
            return Err(Error::TableNotFound {
                table_name: table_name.to_string(),
            });
        }

        // Create new snapshot for this DROP operation
        conn.prepare(INSERT_SNAPSHOT_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                curr_snapshot.schema_version,
                curr_snapshot.next_catalog_id,
                curr_snapshot.next_file_id,
            ])
            .context(QueryExecutionSnafu)?;

        conn.prepare(INSERT_SNAPSHOT_CHANGE_QUERY)
            .context(QueryExecutionSnafu)?
            .execute(params![
                curr_snapshot.id.0 + 1,
                format!(r#"dropped_table:"{}""#, table_name),
                Null,
                Null,
                Null,
            ])
            .context(QueryExecutionSnafu)?;

        Ok(())
    }

    /// Get external table at a specific snapshot (wrapper for backward compatibility).
    fn get_external_table_at_snapshot_inner(
        conn: &Connection,
        schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        Self::get_external_table_inner(conn, schema_name, table_name, Some(snapshot_id))
    }

    /// List external tables at a specific snapshot (wrapper for backward compatibility).
    fn list_external_tables_at_snapshot_inner(
        conn: &Connection,
        schema_name: Option<&str>,
        snapshot_id: i64,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        Self::list_external_tables_inner(conn, schema_name, Some(snapshot_id))
    }

    fn set_table_statistics_inner(
        conn: &Connection,
        _schema_name: Option<&str>,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), Error> {
        let schema_info = Self::current_schema_info_inner(conn)?;

        // Get table_id from ducklake_table or optd_external_table
        let table_id: Result<i64, _> = conn
            .prepare(
                r#"
                SELECT table_id FROM __ducklake_metadata_metalake.main.ducklake_table
                WHERE schema_id = ? AND table_name = ?
                UNION ALL
                SELECT table_id FROM __ducklake_metadata_metalake.main.optd_external_table
                WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL
                LIMIT 1
                "#,
            )
            .context(QueryExecutionSnafu)?
            .query_row(
                params![
                    schema_info.schema_id,
                    table_name,
                    schema_info.schema_id,
                    table_name
                ],
                |row| row.get(0),
            );

        let table_id = match table_id {
            Ok(id) => id,
            Err(DuckDBError::QueryReturnedNoRows) => {
                return Err(Error::TableNotFound {
                    table_name: table_name.to_string(),
                });
            }
            Err(e) => return Err(Error::QueryExecution { source: e }),
        };

        // Check for existing statistics
        let has_existing_stats: i64 = conn
            .prepare(
                r#"
                SELECT COUNT(*) 
                FROM __ducklake_metadata_metalake.main.ducklake_table_stats
                WHERE table_id = ? AND record_count IS NOT NULL
                "#,
            )
            .context(QueryExecutionSnafu)?
            .query_row(params![table_id], |row| row.get(0))
            .context(QueryExecutionSnafu)?;

        let curr_snapshot = if has_existing_stats > 0 {
            // Close existing column stats before creating new snapshot
            let close_snapshot = Self::current_snapshot_info_inner(conn)?;

            conn.prepare(
                r#"
                UPDATE __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                SET end_snapshot = ?
                WHERE end_snapshot IS NULL
                    AND table_id = ?
                "#,
            )
            .context(QueryExecutionSnafu)?
            .execute(params![close_snapshot.id.0, table_id])
            .context(QueryExecutionSnafu)?;

            // Create a new snapshot for the update
            let new_snapshot = Self::current_snapshot_info_inner(conn)?;

            conn.prepare(INSERT_SNAPSHOT_QUERY)
                .context(QueryExecutionSnafu)?
                .execute(params![
                    new_snapshot.id.0 + 1,
                    new_snapshot.schema_version,
                    new_snapshot.next_catalog_id,
                    new_snapshot.next_file_id,
                ])
                .context(QueryExecutionSnafu)?;

            conn.prepare(INSERT_SNAPSHOT_CHANGE_QUERY)
                .context(QueryExecutionSnafu)?
                .execute(params![
                    new_snapshot.id.0 + 1,
                    format!("Updated table statistics for table_id: {}", table_id),
                    Null,
                    Null,
                    Null,
                ])
                .context(QueryExecutionSnafu)?;

            // Return the new snapshot info
            SnapshotInfo {
                id: SnapshotId(new_snapshot.id.0 + 1),
                schema_version: new_snapshot.schema_version,
                next_catalog_id: new_snapshot.next_catalog_id,
                next_file_id: new_snapshot.next_file_id,
            }
        } else {
            // No existing stats, just get current snapshot
            Self::current_snapshot_info_inner(conn)?
        };

        // Insert/update row count in ducklake_table_stats
        // First, delete existing row if any
        conn.prepare(
            r#"
            DELETE FROM __ducklake_metadata_metalake.main.ducklake_table_stats
            WHERE table_id = ?
            "#,
        )
        .context(QueryExecutionSnafu)?
        .execute(params![table_id])
        .context(QueryExecutionSnafu)?;

        // Insert new row count and file size
        conn.prepare(
            r#"
            INSERT INTO __ducklake_metadata_metalake.main.ducklake_table_stats
                (table_id, record_count, next_row_id, file_size_bytes)
            VALUES (?, ?, NULL, ?)
            "#,
        )
        .context(QueryExecutionSnafu)?
        .execute(params![
            table_id,
            stats.row_count as i64,
            stats.size_bytes.map(|s| s as i64)
        ])
        .context(QueryExecutionSnafu)?;

        // Insert column statistics
        for col_stats in &stats.column_statistics {
            // Insert basic statistics (min/max/null/distinct)
            if col_stats.min_value.is_some()
                || col_stats.max_value.is_some()
                || col_stats.null_count.is_some()
                || col_stats.distinct_count.is_some()
            {
                let mut basic_payload = serde_json::json!({});

                if let Value::Object(map) = &mut basic_payload {
                    if col_stats.column_id == 0 {
                        // For external tables, include column_name
                        map.insert("column_name".to_string(), json!(col_stats.name));
                    }
                    if let Some(ref min_val) = col_stats.min_value {
                        map.insert("min_value".to_string(), json!(min_val));
                    }
                    if let Some(ref max_val) = col_stats.max_value {
                        map.insert("max_value".to_string(), json!(max_val));
                    }
                    if let Some(null_cnt) = col_stats.null_count {
                        map.insert("null_count".to_string(), json!(null_cnt));
                    }
                    if let Some(distinct_cnt) = col_stats.distinct_count {
                        map.insert("distinct_count".to_string(), json!(distinct_cnt));
                    }
                }

                conn.prepare(INSERT_ADV_STATS_QUERY)
                    .context(QueryExecutionSnafu)?
                    .execute(params![
                        col_stats.column_id,
                        curr_snapshot.id.0,
                        Null, // end_snapshot
                        table_id,
                        "basic_stats", // stats_type
                        basic_payload.to_string()
                    ])
                    .context(QueryExecutionSnafu)?;
            }

            // Insert advanced statistics (existing behavior)
            for adv_stat in &col_stats.advanced_stats {
                // Build JSON payload
                let mut payload_obj = if col_stats.column_id == 0 {
                    // For external tables (column_id = 0), include column_name in payload
                    serde_json::json!({
                        "column_name": col_stats.name
                    })
                } else {
                    // For internal tables, payload is just the stat data
                    serde_json::json!({})
                };

                // Merge the stat's data into the payload
                if let (Value::Object(map), Value::Object(data_map)) =
                    (&mut payload_obj, &adv_stat.data)
                {
                    for (k, v) in data_map {
                        map.insert(k.clone(), v.clone());
                    }
                }

                conn.prepare(INSERT_ADV_STATS_QUERY)
                    .context(QueryExecutionSnafu)?
                    .execute(params![
                        col_stats.column_id,
                        curr_snapshot.id.0,
                        Null, // end_snapshot
                        table_id,
                        adv_stat.stats_type,
                        payload_obj.to_string()
                    ])
                    .context(QueryExecutionSnafu)?;
            }
        }

        Ok(())
    }
}
