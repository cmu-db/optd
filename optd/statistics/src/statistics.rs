use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ResultExt, prelude::*};
use std::sync::Arc;

use crate::ducklake_connection::{DuckLakeConnectionBuilder, Error as ConnectionError};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database connection error: {}", source))]
    Connection { source: ConnectionError },
    #[snafu(display("Query execution failed: {}", source))]
    QueryExecution { source: duckdb::Error },
    #[snafu(display("JSON serialization error: {}", source))]
    JsonSerialization { source: serde_json::Error },
    #[snafu(display(
        "Statistics not found for table: {}, column: {}, snapshot: {}",
        table,
        column,
        snapshot
    ))]
    StatsNotFound {
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

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Error::Connection { source: err }
    }
}

/** Packaged Statistics Objects */
/** Table statistics -- Contains overall row count and per-column statistics */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    row_count: usize,
    column_statistics: Vec<ColumnStatistics>,
}

impl TableStatistics {
    fn new<I>(rows: I) -> Self
    where
        I: IntoIterator<
            Item = Result<
                (
                    i64,
                    i64,
                    String,
                    String,
                    i64,
                    i64,
                    i64,
                    String,
                    String,
                    String,
                    String,
                    String,
                ),
                duckdb::Error,
            >,
        >,
    {
        let mut row_count = 0;
        let mut column_statistics = Vec::new();

        for row_result in rows {
            if let Ok((
                _table_id,
                column_id,
                column_name,
                column_type,
                record_count,
                _next_row_id,
                _file_size_bytes,
                contains_null,
                contains_nan,
                min_value,
                max_value,
                _extra_stats_json,
            )) = row_result
            {
                row_count = record_count as usize; // Assuming all columns have the same record_count

                let actual_contains_null = match contains_null.as_str() {
                    "TRUE" => Some(true),
                    "FALSE" => Some(false),
                    _ => None,
                };

                let actual_contains_nan = match contains_nan.as_str() {
                    "TRUE" => Some(true),
                    "FALSE" => Some(false),
                    _ => None,
                };

                let actual_min_value = if min_value == "NULL" {
                    None
                } else {
                    Some(min_value)
                };

                let actual_max_value = if max_value == "NULL" {
                    None
                } else {
                    Some(max_value)
                };

                let column_stats = ColumnStatistics::new(
                    column_id,
                    column_type,
                    column_name.clone(),
                    actual_min_value,
                    actual_max_value,
                    actual_contains_null,
                    actual_contains_nan,
                    vec![], // Advanced stats can be populated later
                );

                column_statistics.push(column_stats);
            }
        }

        TableStatistics {
            row_count,
            column_statistics,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    id: i64,
    column_type: String,
    name: String,
    min: Option<String>,
    max: Option<String>,
    contains_null: Option<bool>,
    contains_nan: Option<bool>,
    advanced_stats: Vec<AdvanceColumnStatistics>, // TODO, e.g. histogram, number of distinct values (set cardinality), etc.
}

impl ColumnStatistics {
    fn new(
        id: i64,
        column_type: String,
        name: String,
        min: Option<String>,
        max: Option<String>,
        contains_null: Option<bool>,
        contains_nan: Option<bool>,
        advanced_stats: Vec<AdvanceColumnStatistics>,
    ) -> Self {
        ColumnStatistics {
            id,
            column_type,
            name,
            min,
            max,
            contains_null,
            contains_nan,
            advanced_stats,
        }
    }

    #[allow(dead_code)]
    fn add_advanced_stat(&mut self, stat: AdvanceColumnStatistics) {
        self.advanced_stats.push(stat);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdvanceColumnStatistics {
    stats_type: String,
    data: Value,
}

pub trait StatisticsProvider {
    /// Create a new memory-based StatisticsProvider
    fn memory() -> Result<Box<Self>, Error>;

    /// Create a new file-based StatisticsProvider
    fn file(path: &str) -> Result<Box<Self>, Error>;

    fn get_connection(&self) -> Result<duckdb::Connection, Error>;

    fn current_snapshot(&self, connection: &duckdb::Connection) -> Result<CurrentSnapshot, Error>;

    /// Retrieve table and column statistics at specific snapshot
    fn fetch_table_statistics(
        &self,
        table_name: &str,
        snapshot: i64,
        connection: &duckdb::Connection,
    ) -> Result<Option<TableStatistics>, Error>;

    /// Insert table column statistics
    fn insert_table_stats(
        &self,
        column_id: i64,
        begin_snapshot: i64,
        end_snapshot: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error>;
}

/// DuckLake-based implementation of StatisticsProvider
pub struct DuckLakeStatisticsProvider {
    connection_builder: Arc<DuckLakeConnectionBuilder>,
}

impl DuckLakeStatisticsProvider {
    /// Create a new DuckLakeStatisticsProvider with memory-based DuckDB
    pub fn memory() -> Result<Self, Error> {
        let connection_builder = Arc::new(DuckLakeConnectionBuilder::memory()?);
        Ok(Self { connection_builder })
    }

    /// Create a new DuckLakeStatisticsProvider with file-based DuckDB
    pub fn file(path: &str) -> Result<Self, Error> {
        let connection_builder = Arc::new(DuckLakeConnectionBuilder::file(path)?);
        Ok(Self { connection_builder })
    }
}

pub struct SnapshotId(i64);

pub struct CurrentSnapshot {
    snapshot_id: SnapshotId,
    schema_version: i64,
    next_catalog_id: i64,
    next_file_id: i64,
}

impl StatisticsProvider for DuckLakeStatisticsProvider {
    fn memory() -> Result<Box<Self>, Error> {
        let connection_builder = Arc::new(DuckLakeConnectionBuilder::memory()?);
        Ok(Box::new(Self { connection_builder }))
    }

    /// Create a new DuckLakeStatisticsProvider with file-based DuckDB
    fn file(path: &str) -> Result<Box<Self>, Error> {
        let connection_builder = Arc::new(DuckLakeConnectionBuilder::file(path)?);
        Ok(Box::new(Self { connection_builder }))
    }

    /// Get a connection to the DuckDB instance and initialize the DuckLake-Optd schema
    fn get_connection(&self) -> Result<duckdb::Connection, Error> {
        let conn = self.connection_builder.connect()?;
        self.connection_builder.initialize_schema(&conn)?;
        Ok(conn)
    }

    fn current_snapshot(&self, conn: &duckdb::Connection) -> Result<CurrentSnapshot, Error> {
        let mut stmt = conn
            .prepare(
                format!(
                    r#"
                        SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
                            FROM __ducklake_metadata_{name}.main.ducklake_snapshot
                            WHERE snapshot_id = (SELECT MAX(snapshot_id)
                            FROM __ducklake_metadata_{name}.main.ducklake_snapshot);
                    "#,
                    name = self.connection_builder.get_meta_name()
                )
                .as_str(),
            )
            .context(QueryExecutionSnafu)?;

        let current_snapshot = stmt
            .query_row([], |row| {
                Ok(CurrentSnapshot {
                    snapshot_id: SnapshotId(row.get("snapshot_id")?),
                    schema_version: row.get("schema_version")?,
                    next_catalog_id: row.get("next_catalog_id")?,
                    next_file_id: row.get("next_file_id")?,
                })
            })
            .context(QueryExecutionSnafu)?;

        Ok(current_snapshot)
    }

    fn fetch_table_statistics(
        &self,
        table: &str,
        snapshot: i64,
        conn: &duckdb::Connection,
    ) -> Result<Option<TableStatistics>, Error> {
        // Query for table statistics within the snapshot range
        let mut stmt = conn
            .prepare(
                format!(
                    r#"
                        SELECT table_id, column_id, column_name, column_type, record_count, next_row_id, file_size_bytes, contains_null, contains_nan, min_value, max_value, extra_stats
                            FROM __ducklake_metadata_{name}.main.ducklake_table_stats
                            LEFT JOIN __ducklake_metadata_{name}.main.ducklake_table_column_stats USING (table_id)
                            LEFT JOIN __ducklake_metadata_{name}.main.ducklake_column col USING (table_id, column_id)
                            WHERE record_count IS NOT NULL AND file_size_bytes IS NOT NULL AND
                                table_id = (SELECT table_id FROM __ducklake_metadata_{name}.main.ducklake_table WHERE table_name = ?)
                                AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)
                            ORDER BY table_id, column_id;
                    "#,
                    name = self.connection_builder.get_meta_name()
                ).as_str()
            )
            .context(QueryExecutionSnafu)?;

        let rows = stmt
            .query_map([table, snapshot.to_string().as_str()], |row| {
                Ok((
                    row.get::<usize, i64>(0)?,     // table_id
                    row.get::<usize, i64>(1)?,     // column_id
                    row.get::<usize, String>(2)?,  // column_name
                    row.get::<usize, String>(3)?,  // column_type
                    row.get::<usize, i64>(4)?,     // record_count
                    row.get::<usize, i64>(5)?,     // next_row_id
                    row.get::<usize, i64>(6)?,     // file_size_bytes
                    row.get::<usize, String>(7)?,  // contains_null
                    row.get::<usize, String>(8)?,  // contains_nan
                    row.get::<usize, String>(9)?,  // min_value
                    row.get::<usize, String>(10)?, // max_value
                    row.get::<usize, String>(11)?, // extra_stats (JSON)
                ))
            })
            .context(QueryExecutionSnafu)?;

        let table_stats: TableStatistics = TableStatistics::new(rows);

        Ok(Some(table_stats))
    }

    /// Insert table column statistics
    fn insert_table_stats(
        &self,
        column_id: i64,
        begin_snapshot: i64,
        end_snapshot: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        let mut conn = self.connection_builder.connect()?;
        // let mut txn = conn.transaction().unwrap();

        let current_snapshot = self.current_snapshot(&conn)?;

        // Parameters: column_id, table_id, (stats_type: &str, payload: &str)
        // 1. Get the current snapshot id
        // 2. insert with begin_snapshot = current snapshot + 1;
        // 3. do an update query, UPDATE end_snapshot = current_snapshot WHERE column_id= ? and table_id= ? and stats_type = ?;
        // 4. increment next snapshot id

        // 3. check how duckdb do snapshot id increments (might need update `ducklake_snapshot` table).

        // R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES ({SNAPSHOT_ID}, NOW(), {SCHEMA_VERSION}, {NEXT_CATALOG_ID}, {NEXT_FILE_ID});)");
        // 4. Update snapshot_changes (MIGHT BE optional).
        // auto query = StringUtil::Format(
        //     R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES ({SNAPSHOT_ID}, %s, %s, %s, %s);)",
        //     SQLStringOrNull(change_info.changes_made), commit_info.author.ToSQLString(),
        //     commit_info.commit_message.ToSQLString(), commit_info.commit_extra_info.ToSQLString());
        // auto result = transaction.Query(commit_snapshot, query);
        // if (result->HasError()) {
        // 	result->GetErrorObject().Throw("Failed to write new snapshot to DuckLake:");
        // }
        // Commit

        let table_name = format!(
            "__ducklake_metadata_{}.main.ducklake_table_column_adv_stats",
            self.connection_builder.get_meta_name()
        );

        let query = format!(
            "INSERT INTO {} 
             (column_id, begin_snapshot, end_snapshot, table_id, stats_type, payload) 
             VALUES (?, ?, ?, ?, ?, ?)",
            table_name
        );
        let mut stmt = conn.prepare(&query).context(QueryExecutionSnafu)?;

        stmt.execute(duckdb::params![
            &column_id,
            &begin_snapshot,
            &end_snapshot,
            table_id,
            stats_type,
            payload,
        ])
        .context(QueryExecutionSnafu)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_ducklake_statistics_provider_creation() {
        {
            // Test memory-based provider
            let memory_provider = DuckLakeStatisticsProvider::memory();
            assert!(memory_provider.is_ok());
        }

        {
            // Test file-based provider
            let file_provider = DuckLakeStatisticsProvider::file("./test_stats.db");
            assert!(file_provider.is_ok());
        }
    }

    #[test]
    fn test_table_stats_insertion() {
        let provider = DuckLakeStatisticsProvider::memory().unwrap();

        // Initialize the schema first
        let _conn = provider.get_connection().unwrap();

        // Insert table statistics
        let result =
            provider.insert_table_stats(1, 1, 100, 1, "ndv", r#"{"distinct_count": 1000}"#);
        match &result {
            Ok(_) => println!("Table stats insertion successful"),
            Err(e) => println!("Table stats insertion failed: {}", e),
        }
        assert!(result.is_ok());
    }

    #[test]
    fn test_json_payload_handling() {
        let payload = json!({
            "distinct_count": 1000,
            "null_count": 50,
            "min_value": 1,
            "max_value": 999999
        });

        let payload_str = serde_json::to_string(&payload).unwrap();
        let parsed_back: serde_json::Value = serde_json::from_str(&payload_str).unwrap();

        assert_eq!(parsed_back["distinct_count"], 1000);
        assert_eq!(parsed_back["null_count"], 50);
    }

    #[test]
    fn test_table_stats_insertion_and_retrieval() {
        let provider = DuckLakeStatisticsProvider::memory().unwrap();

        // Initialize the schema first
        let _conn = provider.get_connection().unwrap();

        // Insert table statistics
        let result =
            provider.insert_table_stats(1, 1, 100, 1, "ndv", r#"{"distinct_count": 1000}"#);
        match &result {
            Ok(_) => println!("Table stats insertion successful"),
            Err(e) => println!("Table stats insertion failed: {}", e),
        }
        assert!(result.is_ok());

        // Note: Actual retrieval would require setting up the table_metadata
        // and column_metadata tables, which would be done by the DuckLake extension
    }
}
