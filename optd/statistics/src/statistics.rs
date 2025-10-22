use duckdb::{Connection, Error as DuckDBError, params};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ResultExt, prelude::*};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database connection error: {}", source))]
    Connection { source: DuckDBError },
    #[snafu(display("Query execution failed: {}", source))]
    QueryExecution { source: DuckDBError },
    #[snafu(display("JSON serialization error: {}", source))]
    JsonSerialization { source: serde_json::Error },
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

/** Packaged Statistics Objects */
/** Table statistics -- Contains overall row count and per-column statistics */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    row_count: usize,
    column_statistics: Vec<ColumnStatistics>,
}

impl FromIterator<Result<StatisticsEntry, Error>> for TableStatistics {
    fn from_iter<T: IntoIterator<Item = Result<StatisticsEntry, Error>>>(iter: T) -> Self {
        let mut row_count = 0;
        let mut column_statistics = Vec::new();

        for row_result in iter {
            if let Ok(StatisticsEntry {
                table_id: _,
                column_id,
                column_name,
                column_type,
                record_count,
                next_row_id: _,
                file_size_bytes: _,
                contains_null,
                contains_nan,
                min_value,
                max_value,
                extra_stats: _,
            }) = row_result
            {
                row_count = record_count as usize; // Assuming all columns have the same record_count

                let column_stats = ColumnStatistics::new(
                    column_id,
                    column_type,
                    column_name.clone(),
                    min_value,
                    max_value,
                    contains_null,
                    contains_nan,
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

pub struct SnapshotId(i64);

struct StatisticsEntry {
    table_id: i64,
    column_id: i64,
    column_name: String,
    column_type: String,
    record_count: i64,
    next_row_id: i64,
    file_size_bytes: i64,
    contains_null: Option<bool>,
    contains_nan: Option<bool>,
    min_value: Option<String>,
    max_value: Option<String>,
    extra_stats: Option<String>,
}

pub trait StatisticsProvider {
    fn fetch_current_snapshot(&self) -> Result<SnapshotId, Error>;

    /// Retrieve table and column statistics at specific snapshot
    fn fetch_table_statistics(
        &self,
        table_name: &str,
        snapshot: i64,
        connection: &Connection,
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
    conn: Connection,
}

impl DuckLakeStatisticsProvider {
    /// Create a new DuckLakeStatisticsProvider with memory-based DuckDB
    pub fn try_new(location: Option<&str>) -> Result<Self, Error> {
        let conn = if let Some(path) = location {
            Connection::open(path).context(ConnectionSnafu)?
        } else {
            Connection::open_in_memory().context(ConnectionSnafu)?
        };

        let setup_query = r#"
            INSTALL ducklake;
            LOAD ducklake;
            ATTACH 'ducklake:metadata.ducklake' AS metalake;
            USE metalake;

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats (
                column_id BIGINT,
                begin_snapshot BIGINT,
                end_snapshot BIGINT,
                table_id BIGINT,
                stats_type VARCHAR,
                payload TEXT
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_query (
                query_id BIGINT,
                query_string TEXT,
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
                payload TEXT
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_execution_subplan_feedback (
                group_id BIGINT,
                begin_snapshot BIGINT,
                end_snapshot BIGINT,
                stats_type VARCHAR,
                payload TEXT
            );

            CREATE TABLE IF NOT EXISTS __ducklake_metadata_metalake.main.optd_subplan_scalar_feedback (
                scalar_id BIGINT,
                group_id BIGINT,
                stats_type VARCHAR,
                payload TEXT,
                query_instance_id BIGINT
            );
        "#;
        conn.execute_batch(setup_query).context(ConnectionSnafu)?;
        Ok(Self { conn })
    }

    pub fn get_connection(&self) -> &Connection {
        &self.conn
    }
}

impl StatisticsProvider for DuckLakeStatisticsProvider {
    fn fetch_current_snapshot(&self) -> Result<SnapshotId, Error> {
        let mut stmt = self
            .conn
            .prepare("FROM ducklake_current_snapshot('metalake');")
            .context(QueryExecutionSnafu)?;

        let snapshot_id = stmt
            .query_row([], |row| Ok(SnapshotId(row.get(0)?)))
            .context(QueryExecutionSnafu)?;

        Ok(snapshot_id)
    }

    fn fetch_table_statistics(
        &self,
        table: &str,
        snapshot: i64,
        conn: &Connection,
    ) -> Result<Option<TableStatistics>, Error> {
        // Query for table statistics within the snapshot range
        let mut stmt = conn
            .prepare(
                    r#"
                        SELECT 
                            ts.table_id, 
                            tcs.column_id, 
                            dc.column_name, 
                            dc.column_type, 
                            ts.record_count, 
                            ts.next_row_id, 
                            ts.file_size_bytes, 
                            tcs.contains_null, 
                            tcs.contains_nan, 
                            tcs.min_value, 
                            tcs.max_value, 
                            tcs.extra_stats
                        FROM __ducklake_metadata_metalake.main.ducklake_table_stats ts
                        LEFT JOIN __ducklake_metadata_metalake.main.ducklake_table_column_stats tcs USING (table_id)
                        LEFT JOIN __ducklake_metadata_metalake.main.ducklake_column dc USING (table_id, column_id)
                        INNER JOIN __ducklake_metadata_metalake.main.ducklake_table dt ON ts.table_id = dt.table_id
                        INNER JOIN __ducklake_metadata_metalake.main.ducklake_schema ds ON dt.schema_id = ds.schema_id
                        WHERE 
                            ds.schema_name = current_schema()
                            AND dt.table_name = ?
                            AND ts.record_count IS NOT NULL 
                            AND ts.file_size_bytes IS NOT NULL
                            AND ? >= dc.begin_snapshot 
                            AND (? < dc.end_snapshot OR dc.end_snapshot IS NULL)
                        ORDER BY ts.table_id, tcs.column_id;
                    "#
            )
            .context(QueryExecutionSnafu)?;

        let entries = stmt
            .query_map([snapshot.to_string().as_str(), table], |row| {
                Ok(StatisticsEntry {
                    table_id: row.get("column_id")?,
                    column_id: row.get("column_id")?,
                    column_name: row.get("column_name")?,
                    column_type: row.get("column_type")?,
                    record_count: row.get("record_count")?,
                    next_row_id: row.get("next_row_id")?,
                    file_size_bytes: row.get("file_size_bytes")?,
                    contains_null: row.get("contains_null")?,
                    contains_nan: row.get("contains_nan")?,
                    min_value: row.get("min_value")?,
                    max_value: row.get("max_value")?,
                    extra_stats: row.get("extra_stats")?,
                })
            })
            .context(QueryExecutionSnafu)?
            .map(|result| result.context(QueryExecutionSnafu));

        let table_stats: TableStatistics = TableStatistics::from_iter(entries);

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
        // let mut txn = conn.transaction().unwrap();

        let current_snapshot = self.fetch_current_snapshot()?;

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

        let query = "INSERT OR REPLACE INTO __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
             (column_id, begin_snapshot, end_snapshot, table_id, stats_type, payload) 
             VALUES (?, ?, ?, ?, ?, ?)";

        let mut stmt = self.conn.prepare(&query).context(QueryExecutionSnafu)?;

        stmt.execute(params![
            column_id,
            begin_snapshot,
            end_snapshot,
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
    use tempfile::TempDir;

    #[test]
    fn test_ducklake_statistics_provider_creation() {
        {
            // Test memory-based provider
            let memory_provider = DuckLakeStatisticsProvider::try_new(None);
            assert!(memory_provider.is_ok());
        }

        {
            // Test file-based provider with temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test_stats.db");
            let file_provider =
                DuckLakeStatisticsProvider::try_new(Some(db_path.to_str().unwrap()));
            assert!(file_provider.is_ok());
        }
    }

    #[test]
    fn test_table_stats_insertion() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_insertion.db");
        let provider =
            DuckLakeStatisticsProvider::try_new(Some(db_path.to_str().unwrap())).unwrap();

        // Insert table statistics
        let result =
            provider.insert_table_stats(1, 1, 100, 1, "ndv", r#"{"distinct_count": 1000}"#);
        match &result {
            Ok(_) => println!("Table stats insertion successful"),
            Err(e) => println!("Table stats insertion failed: {}", e),
        }
        assert!(result.is_ok());
        // temp_dir is automatically cleaned up when it goes out of scope
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
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_retrieval.db");
        let provider =
            DuckLakeStatisticsProvider::try_new(Some(db_path.to_str().unwrap())).unwrap();

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
