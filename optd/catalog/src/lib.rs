use duckdb::{
    Connection, Error as DuckDBError,
    arrow::datatypes::{Field, Schema, SchemaRef},
    params,
    types::Null,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
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
        let result = Self::table_statistics_inner(&txn, table, snapshot);
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

    fn table_statistics_inner(
        conn: &Connection,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<TableStatistics>, Error> {
        let mut stmt = conn
            .prepare(FETCH_TABLE_STATS_QUERY)
            .context(QueryExecutionSnafu)?;

        let entries = stmt
            .query_map(
                params![&snapshot.0, &snapshot.0, table, &snapshot.0, &snapshot.0,],
                |row| {
                    Ok(TableColumnStatisticsEntry {
                        _table_id: row.get("table_id")?,
                        column_id: row.get("column_id")?,
                        column_name: row.get("column_name")?,
                        column_type: row.get("column_type")?,
                        record_count: row.get("record_count")?,
                        _next_row_id: row.get("next_row_id")?,
                        _file_size_bytes: row.get("file_size_bytes")?,
                        stats_type: row.get("stats_type")?,
                        payload: row.get("payload")?,
                    })
                },
            )
            .context(QueryExecutionSnafu)?
            .map(|result| result.context(QueryExecutionSnafu));

        Ok(Some(TableStatistics::from_iter(entries)))
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
}
