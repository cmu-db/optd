use duckdb::{Connection, Error as DuckDBError, params, types::Null};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ResultExt, prelude::*};

const DEFAULT_METADATA_FILE: &str = "metadata.ducklake";

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

pub struct SnapshotInfo {
    snapshot_id: i64,
    schema_version: i64,
    next_catalog_id: i64,
    next_file_id: i64,
}

pub struct CurrentSchema {
    schema_name: String,
    schema_id: i64,
    begin_snapshot: i64,
    end_snapshot: i64,
}

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

#[derive(Debug, Serialize, Deserialize)]
struct StatisticsUpdate {
    stats_type: String,
    payload: String,
}

pub trait StatisticsProvider {
    fn fetch_current_snapshot(&self) -> Result<SnapshotId, Error>;

    fn fetch_current_snapshot_info(&self) -> Result<SnapshotInfo, Error>;

    fn fetch_current_schema(&self) -> Result<CurrentSchema, Error>;

    /// Retrieve table and column statistics at specific snapshot
    fn fetch_table_statistics(
        &self,
        table_name: &str,
        snapshot: i64,
        connection: &Connection,
    ) -> Result<Option<TableStatistics>, Error>;

    /// Insert table column statistics
    fn update_table_column_stats(
        &self,
        column_id: i64,
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
    /// Parameters:
    /// - location: Optional path to database file
    /// - metadata_path: Optional path to ducklake metadata file
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
        "#,
            metadata_file
        );
        conn.execute_batch(&setup_query).context(ConnectionSnafu)?;
        Ok(Self { conn })
    }

    pub fn get_connection(&self) -> &Connection {
        &self.conn
    }

    fn begin_transaction(&self) -> Result<(), Error> {
        let mut begin_txn_stmt = self
            .conn
            .prepare("BEGIN TRANSACTION;")
            .context(QueryExecutionSnafu)?;
        begin_txn_stmt.execute([]).context(QueryExecutionSnafu)?;
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), Error> {
        let mut commit_txn_stmt = self
            .conn
            .prepare("COMMIT TRANSACTION;")
            .context(QueryExecutionSnafu)?;
        commit_txn_stmt.execute([]).context(QueryExecutionSnafu)?;
        Ok(())
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

    fn fetch_current_snapshot_info(&self) -> Result<SnapshotInfo, Error> {
        let mut snapshot_stmt = self
            .conn
            .prepare(
                r#"
                        SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
                            FROM __ducklake_metadata_metalake.main.ducklake_snapshot
                            WHERE snapshot_id = (SELECT MAX(snapshot_id)
                                FROM __ducklake_metadata_metalake.main.ducklake_snapshot);
                    "#,
            )
            .context(QueryExecutionSnafu)?;

        let current_snapshot_info = snapshot_stmt
            .query_row([], |row| {
                Ok(SnapshotInfo {
                    snapshot_id: row.get("snapshot_id")?,
                    schema_version: row.get("schema_version")?,
                    next_catalog_id: row.get("next_catalog_id")?,
                    next_file_id: row.get("next_file_id")?,
                })
            })
            .context(QueryExecutionSnafu)?;

        Ok(current_snapshot_info)
    }

    fn fetch_current_schema(&self) -> Result<CurrentSchema, Error> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT ds.schema_id, ds.schema_name, ds.begin_snapshot, ds.end_snapshot
                    FROM __ducklake_metadata_metalake.main.ducklake_schema ds
                    WHERE ds.schema_name = current_schema();
            "#,
            )
            .context(QueryExecutionSnafu)?;

        let snapshot_id = stmt
            .query_row([], |row| {
                Ok(CurrentSchema {
                    schema_name: row.get("schema_name")?,
                    schema_id: row.get("schema_id")?,
                    begin_snapshot: row.get("begin_snapshot")?,
                    end_snapshot: row.get("end_snapshot")?,
                })
            })
            .context(QueryExecutionSnafu)?;

        Ok(snapshot_id)
    }

    fn fetch_table_statistics(
        &self,
        table: &str,
        snapshot: i64,
        conn: &Connection,
    ) -> Result<Option<TableStatistics>, Error> {
        // Query for table statistics at the snapshot
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
        let mut update_stmt = self
            .conn
            .prepare(
                r#"
            UPDATE __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                SET end_snapshot = ?
                WHERE end_snapshot IS NULL
                    AND stats_type = ?
                    AND column_id = ?
                    AND table_id = ?;
            "#,
            )
            .context(QueryExecutionSnafu)?;

        update_stmt
            .execute(params![
                current_snapshot_id,
                stats_type,
                column_id,
                table_id,
            ])
            .context(QueryExecutionSnafu)?;

        // Insert new snapshot
        let mut insert_stmt = self
            .conn
            .prepare(
                r#"
            INSERT INTO __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                (column_id, begin_snapshot, end_snapshot, table_id, stats_type, payload) 
                VALUES (?, ?, ?, ?, ?, ?);
            "#,
            )
            .context(QueryExecutionSnafu)?;

        insert_stmt
            .execute(params![
                column_id,
                current_snapshot_id + 1,
                Null,
                table_id,
                stats_type,
                payload,
            ])
            .context(QueryExecutionSnafu)?;

        let mut new_snap_stmt = self
            .conn
            .prepare(
                r#"
                INSERT INTO __ducklake_metadata_metalake.main.ducklake_snapshot
                    (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) 
                    VALUES (?, NOW(), ?, ?, ?);
            "#,
            )
            .context(QueryExecutionSnafu)?;

        new_snap_stmt
            .execute(params![
                current_snapshot_id + 1,
                current_snapshot.schema_version,
                current_snapshot.next_catalog_id,
                current_snapshot.next_file_id,
            ])
            .context(QueryExecutionSnafu)?;

        let mut new_snap_change_stmt = self
            .conn
            .prepare(
                r#"
                INSERT INTO __ducklake_metadata_metalake.main.ducklake_snapshot_changes
                    (snapshot_id, changes_made, author, commit_message, commit_extra_info)
                    VALUES (?, ?, ?, ?, ?);
            "#,
            )
            .context(QueryExecutionSnafu)?;

        new_snap_change_stmt
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    // Counter to ensure unique database names
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn create_test_provider(for_file: bool) -> (TempDir, DuckLakeStatisticsProvider) {
        // Create a unique subdirectory to separate DuckLake metadata for each test
        let temp_dir = TempDir::new().unwrap();
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_dir = temp_dir
            .path()
            .join(format!("db_{}_{}", timestamp, counter));
        std::fs::create_dir_all(&unique_dir).unwrap();
        let metadata_path = unique_dir.join("metadata.ducklake");
        if !for_file {
            let provider =
                DuckLakeStatisticsProvider::try_new(None, Some(metadata_path.to_str().unwrap()))
                    .unwrap();
            (temp_dir, provider)
        } else {
            let db_path = unique_dir.join("test.db");
            let provider = DuckLakeStatisticsProvider::try_new(
                Some(db_path.to_str().unwrap()),
                Some(metadata_path.to_str().unwrap()),
            )
            .unwrap();
            (temp_dir, provider)
        }
    }

    #[test]
    fn test_ducklake_statistics_provider_creation() {
        {
            // Test memory-based provider
            let _memory_provider = create_test_provider(false);
            // The provider creation is already asserted in create_test_provider
        }

        {
            // Test file-based provider with unique temporary database
            let (_temp_dir, _provider) = create_test_provider(true);
            // The provider creation is already asserted in create_test_provider
        }
    }

    #[test]
    fn test_table_stats_insertion() {
        let (_temp_dir, provider) = create_test_provider(true);

        // Insert table statistics
        let result = provider.update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#);
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
        let (_temp_dir, provider) = create_test_provider(true);

        // Insert table statistics
        let result = provider.update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#);
        match &result {
            Ok(_) => println!("Table stats insertion successful"),
            Err(e) => println!("Table stats insertion failed: {}", e),
        }
        assert!(result.is_ok());

        // Note: Actual retrieval would require setting up the table_metadata
        // and column_metadata tables, which would be done by the DuckLake extension
    }

    #[test]
    fn test_snapshot_versioning_and_stats_types() {
        let (_temp_dir, provider) = create_test_provider(true);
        let conn = provider.get_connection();

        // Test 1: Multiple columns with sequential snapshots
        provider
            .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#)
            .unwrap();
        provider
            .update_table_column_stats(2, 1, "ndv", r#"{"distinct_count": 2000}"#)
            .unwrap();
        provider
            .update_table_column_stats(3, 1, "histogram", r#"{"buckets": [1,2,3]}"#)
            .unwrap();

        // Verify different columns have sequential snapshots
        let mut stmt = conn
            .prepare(
                r#"
                SELECT column_id, begin_snapshot
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = 1
                ORDER BY begin_snapshot;
            "#,
            )
            .unwrap();
        let snapshots: Vec<(i64, i64)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(snapshots.len(), 3);
        assert!(snapshots[1].1 > snapshots[0].1);
        assert!(snapshots[2].1 > snapshots[1].1);

        // Test 2: Update same column multiple times - verify snapshot continuity
        provider
            .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1500}"#)
            .unwrap();
        provider
            .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 2000}"#)
            .unwrap();

        let mut version_stmt = conn
            .prepare(
                r#"
                SELECT begin_snapshot, end_snapshot, payload
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = 1 AND column_id = 1 AND stats_type = 'ndv'
                ORDER BY begin_snapshot;
            "#,
            )
            .unwrap();
        let versions: Vec<(i64, Option<i64>, String)> = version_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        // Should have 3 versions (original + 2 updates)
        assert_eq!(versions.len(), 3);

        // First two closed, last one current
        assert!(versions[0].1.is_some());
        assert!(versions[1].1.is_some());
        assert!(versions[2].1.is_none());

        // Verify snapshot continuity
        assert_eq!(versions[0].1.unwrap() + 1, versions[1].0);
        assert_eq!(versions[1].1.unwrap() + 1, versions[2].0);

        // Verify payloads updated correctly
        assert!(versions[0].2.contains("1000"));
        assert!(versions[1].2.contains("1500"));
        assert!(versions[2].2.contains("2000"));

        // Test 3: Multiple stat types for same column coexist
        provider
            .update_table_column_stats(1, 1, "histogram", r#"{"buckets": [1,2,3,4,5]}"#)
            .unwrap();
        provider
            .update_table_column_stats(1, 1, "minmax", r#"{"min": 0, "max": 100}"#)
            .unwrap();

        let type_count: i64 = conn
            .query_row(
                r#"
                SELECT COUNT(DISTINCT stats_type)
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                WHERE table_id = 1 AND column_id = 1 AND end_snapshot IS NULL
                "#,
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(type_count, 3); // ndv, histogram, minmax
    }

    #[test]
    fn test_snapshot_tracking_and_multi_table_stats() {
        let (_temp_dir, provider) = create_test_provider(true);
        let conn = provider.get_connection();

        // Get initial snapshot count
        let initial_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_snapshot",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Test 1: Snapshot creation tracking - insert stats for 3 columns
        provider
            .update_table_column_stats(1, 1, "ndv", r#"{"distinct_count": 1000}"#)
            .unwrap();
        provider
            .update_table_column_stats(2, 1, "ndv", r#"{"distinct_count": 2000}"#)
            .unwrap();
        provider
            .update_table_column_stats(3, 1, "ndv", r#"{"distinct_count": 3000}"#)
            .unwrap();

        let after_table1_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_snapshot",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after_table1_count - initial_count, 3);

        // Verify snapshot_changes were recorded
        let changes_count: i64 = conn
            .query_row(
                r#"
                SELECT COUNT(*) 
                FROM __ducklake_metadata_metalake.main.ducklake_snapshot_changes
                WHERE changes_made LIKE 'updated_stats:%'
                "#,
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(changes_count, 3);

        // Test 2: Multiple tables with independent tracking

        // Test 2: Multiple tables with independent tracking
        provider
            .update_table_column_stats(1, 2, "ndv", r#"{"distinct_count": 5000}"#)
            .unwrap();
        provider
            .update_table_column_stats(2, 2, "ndv", r#"{"distinct_count": 6000}"#)
            .unwrap();

        // Verify each table has correct number of stats
        let table1_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats WHERE table_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let table2_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats WHERE table_id = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(table1_count, 3); // 3 columns from table 1
        assert_eq!(table2_count, 2); // 2 columns from table 2

        // Verify all snapshots are sequential across tables
        let mut snapshot_stmt = conn
            .prepare(
                r#"
                SELECT table_id, column_id, begin_snapshot
                FROM __ducklake_metadata_metalake.main.ducklake_table_column_adv_stats
                ORDER BY begin_snapshot
                "#,
            )
            .unwrap();
        let all_snapshots: Vec<i64> = snapshot_stmt
            .query_map([], |row| row.get(2))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        // All 5 snapshots should be increasing
        for i in 1..all_snapshots.len() {
            assert!(all_snapshots[i] > all_snapshots[i - 1]);
        }
    }
}
