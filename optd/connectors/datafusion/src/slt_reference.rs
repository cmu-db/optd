use std::fmt;
#[cfg(feature = "duckdb")]
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_sqllogictest::{
    DFColumnType, DFSqlLogicTestError, convert_batches, convert_schema_to_types,
};
use sqllogictest::{AsyncDB, DBOutput};

#[cfg(feature = "duckdb")]
use crate::setup::{JOB_DATA_DIR, JOB_TABLES, TPCH_DATA_DIR, TPCH_TABLES, parquet_path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedEngine {
    OptdDataFusion,
    DataFusion,
    DuckDb,
}

impl FromStr for ExpectedEngine {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "optd-datafusion" => Ok(Self::OptdDataFusion),
            "datafusion" => Ok(Self::DataFusion),
            "duckdb" => Ok(Self::DuckDb),
            _ => Err(format!(
                "unknown engine {value:?}; expected one of: optd-datafusion, datafusion, duckdb"
            )),
        }
    }
}

impl fmt::Display for ExpectedEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OptdDataFusion => "optd-datafusion",
            Self::DataFusion => "datafusion",
            Self::DuckDb => "duckdb",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SltArgs {
    pub override_files: bool,
    pub filters: Vec<String>,
    pub expected_engine: ExpectedEngine,
}

impl Default for SltArgs {
    fn default() -> Self {
        Self {
            override_files: false,
            filters: Vec::new(),
            expected_engine: ExpectedEngine::OptdDataFusion,
        }
    }
}

pub fn parse_slt_args(args: impl IntoIterator<Item = String>) -> Result<SltArgs, String> {
    let mut parsed = SltArgs::default();
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--override" => parsed.override_files = true,
            "--engine" => {
                let engine = iter
                    .next()
                    .ok_or_else(|| "--engine requires a value".to_string())?;
                parsed.expected_engine = engine.parse()?;
            }
            _ if parsed.override_files && !arg.starts_with("--") => parsed.filters.push(arg),
            _ => {}
        }
    }

    Ok(parsed)
}

pub struct DataFusionRunner {
    session: SessionContext,
}

impl DataFusionRunner {
    pub fn new(session: SessionContext) -> Self {
        Self { session }
    }
}

#[async_trait]
impl AsyncDB for DataFusionRunner {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>, DFSqlLogicTestError> {
        let plan = self
            .session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        let df = self
            .session
            .execute_logical_plan(plan)
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        let schema = df.schema().inner().clone();
        let batches = df
            .collect()
            .await
            .map_err(DFSqlLogicTestError::DataFusion)?;
        let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
        let types = convert_schema_to_types(schema.fields());
        let rows = convert_batches(&schema, batches, false)?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    fn engine_name(&self) -> &str {
        "datafusion"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}

#[cfg(feature = "duckdb")]
pub struct DuckDbRunner {
    conn: duckdb::Connection,
}

#[cfg(feature = "duckdb")]
impl DuckDbRunner {
    pub fn new_for_path(path: &Path) -> Result<Self, DFSqlLogicTestError> {
        let conn = duckdb::Connection::open_in_memory().map_err(duckdb_error)?;
        let runner = Self { conn };

        if path.components().any(|c| c.as_os_str() == "tpch") {
            runner.register_parquet_tables(TPCH_TABLES, TPCH_DATA_DIR, "TPC-H")?;
        } else if path.components().any(|c| c.as_os_str() == "job") {
            runner.register_parquet_tables(JOB_TABLES, JOB_DATA_DIR, "JOB")?;
        }

        Ok(runner)
    }

    fn register_parquet_tables(
        &self,
        tables: &[&str],
        data_dir: &str,
        dataset_name: &str,
    ) -> Result<(), DFSqlLogicTestError> {
        for table in tables {
            let path = parquet_path(data_dir, table);
            if !path.exists() {
                return Err(DFSqlLogicTestError::Other(format!(
                    "missing {dataset_name} parquet data at {}",
                    path.display()
                )));
            }
            let escaped_path = path.to_string_lossy().replace('\'', "''");
            let sql =
                format!("CREATE VIEW {table} AS SELECT * FROM read_parquet('{escaped_path}')");
            self.conn.execute_batch(&sql).map_err(duckdb_error)?;
        }
        Ok(())
    }
}

#[cfg(feature = "duckdb")]
#[async_trait]
impl AsyncDB for DuckDbRunner {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>, DFSqlLogicTestError> {
        duckdb_run(&self.conn, sql)
    }

    fn engine_name(&self) -> &str {
        "duckdb"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}

#[cfg(feature = "duckdb")]
fn duckdb_run(
    conn: &duckdb::Connection,
    sql: &str,
) -> Result<DBOutput<DFColumnType>, DFSqlLogicTestError> {
    let mut stmt = conn.prepare(sql).map_err(duckdb_error)?;
    let mut rows = stmt.query([]).map_err(duckdb_error)?;
    let stmt = rows
        .as_ref()
        .ok_or_else(|| DFSqlLogicTestError::Other("DuckDB query has no statement".to_string()))?;
    if stmt.column_count() == 0 {
        return Ok(DBOutput::StatementComplete(0));
    }

    let types = (0..stmt.column_count())
        .map(|idx| duckdb_column_type(stmt.column_type(idx)))
        .collect::<Vec<_>>();
    let mut output_rows = Vec::new();
    while let Some(row) = rows.next().map_err(duckdb_error)? {
        let mut output_row = Vec::with_capacity(types.len());
        for idx in 0..types.len() {
            let value = row.get_ref(idx).map_err(duckdb_error)?;
            output_row.push(duckdb_value_to_string(value)?);
        }
        output_rows.push(output_row);
    }

    if output_rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows {
            types,
            rows: output_rows,
        })
    }
}

#[cfg(feature = "duckdb")]
fn duckdb_column_type(value: duckdb::arrow::datatypes::DataType) -> DFColumnType {
    use duckdb::arrow::datatypes::DataType;

    match &value {
        DataType::Boolean => DFColumnType::Boolean,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DFColumnType::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => DFColumnType::Float,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DFColumnType::Text,
        DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
            DFColumnType::DateTime
        }
        DataType::Timestamp(_, _) => DFColumnType::Timestamp,
        _ => DFColumnType::Another,
    }
}

#[cfg(feature = "duckdb")]
fn duckdb_value_to_string(
    value: duckdb::types::ValueRef<'_>,
) -> Result<String, DFSqlLogicTestError> {
    use duckdb::types::ValueRef;

    Ok(match value {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Boolean(value) => value.to_string(),
        ValueRef::TinyInt(value) => value.to_string(),
        ValueRef::SmallInt(value) => value.to_string(),
        ValueRef::Int(value) => value.to_string(),
        ValueRef::BigInt(value) => value.to_string(),
        ValueRef::HugeInt(value) => value.to_string(),
        ValueRef::UTinyInt(value) => value.to_string(),
        ValueRef::USmallInt(value) => value.to_string(),
        ValueRef::UInt(value) => value.to_string(),
        ValueRef::UBigInt(value) => value.to_string(),
        ValueRef::Float(value) => value.to_string(),
        ValueRef::Double(value) => value.to_string(),
        ValueRef::Decimal(value) => value.to_string(),
        ValueRef::Timestamp(unit, value) => format_timestamp(unit, value)?,
        ValueRef::Text(value) => std::str::from_utf8(value)
            .map_err(|err| DFSqlLogicTestError::Other(err.to_string()))?
            .to_string(),
        ValueRef::Blob(value) => format!("{value:?}"),
        ValueRef::Date32(value) => {
            duckdb::arrow::array::temporal_conversions::date32_to_datetime(value)
                .map(|value| value.date().to_string())
                .ok_or_else(|| {
                    DFSqlLogicTestError::Other(format!("invalid DuckDB date32 value {value}"))
                })?
        }
        ValueRef::Time64(unit, value) => format_time(unit, value)?,
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => format!("{months} months {days} days {nanos} ns"),
        ValueRef::Enum(_, _) => value.as_str().map_err(duckdb_from_sql_error)?.to_string(),
        value => format!("{value:?}"),
    })
}

#[cfg(feature = "duckdb")]
fn format_timestamp(
    unit: duckdb::types::TimeUnit,
    value: i64,
) -> Result<String, DFSqlLogicTestError> {
    use duckdb::arrow::array::temporal_conversions::{
        timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
        timestamp_us_to_datetime,
    };
    use duckdb::types::TimeUnit;

    let datetime = match unit {
        TimeUnit::Second => timestamp_s_to_datetime(value),
        TimeUnit::Millisecond => timestamp_ms_to_datetime(value),
        TimeUnit::Microsecond => timestamp_us_to_datetime(value),
        TimeUnit::Nanosecond => timestamp_ns_to_datetime(value),
    };
    datetime.map(|value| value.to_string()).ok_or_else(|| {
        DFSqlLogicTestError::Other(format!("invalid DuckDB timestamp value {value}"))
    })
}

#[cfg(feature = "duckdb")]
fn format_time(unit: duckdb::types::TimeUnit, value: i64) -> Result<String, DFSqlLogicTestError> {
    use duckdb::arrow::array::temporal_conversions::{time64ns_to_time, time64us_to_time};
    use duckdb::types::TimeUnit;

    let time = match unit {
        TimeUnit::Second => time64us_to_time(value * 1_000_000),
        TimeUnit::Millisecond => time64us_to_time(value * 1000),
        TimeUnit::Microsecond => time64us_to_time(value),
        TimeUnit::Nanosecond => time64ns_to_time(value),
    };
    time.map(|value| value.to_string())
        .ok_or_else(|| DFSqlLogicTestError::Other(format!("invalid DuckDB time64 value {value}")))
}

#[cfg(feature = "duckdb")]
fn duckdb_error(error: duckdb::Error) -> DFSqlLogicTestError {
    DFSqlLogicTestError::Other(error.to_string())
}

#[cfg(feature = "duckdb")]
fn duckdb_from_sql_error(error: duckdb::types::FromSqlError) -> DFSqlLogicTestError {
    DFSqlLogicTestError::Other(error.to_string())
}

pub fn duckdb_feature_error() -> DFSqlLogicTestError {
    DFSqlLogicTestError::Other(
        "DuckDB expected-output generation requires rerunning with --features duckdb".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqllogictest::AsyncDB;

    fn parse(args: &[&str]) -> Result<SltArgs, String> {
        parse_slt_args(args.iter().map(|arg| (*arg).to_string()))
    }

    #[test]
    fn parse_defaults_to_optd_datafusion() {
        assert_eq!(parse(&[]).unwrap(), SltArgs::default());
    }

    #[test]
    fn parse_datafusion_engine() {
        let args = parse(&["--override", "--engine", "datafusion"]).unwrap();
        assert!(args.override_files);
        assert_eq!(args.expected_engine, ExpectedEngine::DataFusion);
    }

    #[test]
    fn parse_duckdb_engine() {
        let args = parse(&["--override", "--engine", "duckdb"]).unwrap();
        assert_eq!(args.expected_engine, ExpectedEngine::DuckDb);
    }

    #[test]
    fn parse_optd_datafusion_engine() {
        let args = parse(&["--override", "--engine", "optd-datafusion"]).unwrap();
        assert_eq!(args.expected_engine, ExpectedEngine::OptdDataFusion);
    }

    #[test]
    fn parse_rejects_unknown_engine() {
        let err = parse(&["--engine", "sqlite"]).unwrap_err();
        assert!(err.contains("unknown engine"));
        assert!(err.contains("optd-datafusion, datafusion, duckdb"));
    }

    #[test]
    fn parse_preserves_override_filter_behavior() {
        let args = parse(&["ignored", "--override", "tpch", "--flag", "features/values"]).unwrap();
        assert_eq!(args.filters, vec!["tpch", "features/values"]);
    }

    #[tokio::test]
    async fn datafusion_runner_returns_query_rows() {
        let mut runner = DataFusionRunner::new(SessionContext::new());
        let output = runner.run("VALUES (1, 'a'), (2, 'b')").await.unwrap();

        match output {
            DBOutput::Rows { types, rows } => {
                assert_eq!(types, vec![DFColumnType::Integer, DFColumnType::Text]);
                assert_eq!(
                    rows,
                    vec![
                        vec!["1".to_string(), "a".to_string()],
                        vec!["2".to_string(), "b".to_string()],
                    ]
                );
            }
            DBOutput::StatementComplete(_) => panic!("expected query rows"),
            _ => panic!("unexpected DB output"),
        }
    }
}
