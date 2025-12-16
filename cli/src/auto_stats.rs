//! Automatic statistics computation for external tables.
//!
//! Provides functionality to compute table statistics after CREATE EXTERNAL TABLE.
//! Supports fast metadata extraction from Parquet files and sample-based estimation
//! for CSV/JSON files. Behavior is configurable via environment variables.

use datafusion::common::{DataFusionError, Result};
use optd_catalog::TableStatistics;
use std::path::Path;

/// Configuration for automatic statistics computation
#[derive(Debug, Clone)]
pub struct AutoStatsConfig {
    /// Enable automatic statistics computation
    pub enabled: bool,
    /// Enable for Parquet files (fast, metadata-based)
    pub parquet_enabled: bool,
    /// Enable for CSV files (slower, requires sampling)
    pub csv_enabled: bool,
    /// Enable for JSON files (slower, requires sampling)
    pub json_enabled: bool,
    /// Sample size for CSV/JSON (number of rows to scan)
    pub sample_size: usize,
}

impl Default for AutoStatsConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var("OPTD_AUTO_STATS")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(true),
            parquet_enabled: std::env::var("OPTD_AUTO_STATS_PARQUET")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(true),
            csv_enabled: std::env::var("OPTD_AUTO_STATS_CSV")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false), // Disabled by default due to cost
            json_enabled: std::env::var("OPTD_AUTO_STATS_JSON")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false), // Disabled by default due to cost
            sample_size: std::env::var("OPTD_AUTO_STATS_SAMPLE_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10000),
        }
    }
}

impl AutoStatsConfig {
    /// Check if auto-stats is enabled for a given file format
    pub fn is_enabled_for_format(&self, file_format: &str) -> bool {
        if !self.enabled {
            return false;
        }

        match file_format.to_uppercase().as_str() {
            "PARQUET" => self.parquet_enabled,
            "CSV" => self.csv_enabled,
            "JSON" | "NDJSON" => self.json_enabled,
            _ => false,
        }
    }
}

/// Computes statistics for an external table.
///
/// Attempts to compute basic statistics based on file format:
/// - Parquet: Extract row count and column stats from metadata (fast)
/// - CSV/JSON: Sample-based estimation (slower, configurable)
pub async fn compute_table_statistics(
    location: &str,
    file_format: &str,
    config: &AutoStatsConfig,
) -> Result<Option<TableStatistics>> {
    if !config.is_enabled_for_format(file_format) {
        return Ok(None);
    }

    match file_format.to_uppercase().as_str() {
        "PARQUET" => extract_parquet_statistics(location).await,
        "CSV" => compute_csv_statistics(location, config.sample_size).await,
        "JSON" | "NDJSON" => compute_json_statistics(location, config.sample_size).await,
        _ => Ok(None),
    }
}

/// Extracts statistics from Parquet file metadata.
///
/// Parquet stores row count and column statistics in metadata, making this very fast.
async fn extract_parquet_statistics(location: &str) -> Result<Option<TableStatistics>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let path = Path::new(location);
    if !path.exists() {
        return Err(DataFusionError::Execution(format!(
            "Parquet file not found: {}",
            location
        )));
    }

    let file = File::open(path)
        .map_err(|e| DataFusionError::Execution(format!("Failed to open Parquet file: {}", e)))?;

    let reader = SerializedFileReader::new(file).map_err(|e| {
        DataFusionError::Execution(format!("Failed to read Parquet metadata: {}", e))
    })?;

    let metadata = reader.metadata();
    let row_count = metadata.file_metadata().num_rows() as usize;

    // Get file size for I/O cost estimation
    let size_bytes = std::fs::metadata(path).ok().map(|m| m.len() as usize);

    // Extract column statistics from Parquet metadata
    let column_statistics = extract_column_statistics_from_parquet(metadata)?;

    Ok(Some(TableStatistics {
        row_count,
        column_statistics,
        size_bytes,
    }))
}

/// Extract column-level statistics from Parquet metadata
///
/// Aggregates min/max/null_count across all row groups for each column.
fn extract_column_statistics_from_parquet(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<Vec<optd_catalog::ColumnStatistics>> {
    let schema = metadata.file_metadata().schema_descr();
    let num_row_groups = metadata.num_row_groups();
    let mut column_statistics = Vec::new();

    for col_idx in 0..schema.num_columns() {
        let field = &schema.columns()[col_idx];
        let col_name = field.name().to_string();
        let col_type = format!("{:?}", field.physical_type());

        // Aggregate statistics across all row groups
        let mut global_min: Option<String> = None;
        let mut global_max: Option<String> = None;
        let mut total_null_count: usize = 0;
        let mut distinct_count: Option<usize> = None;

        for rg_idx in 0..num_row_groups {
            let row_group = metadata.row_group(rg_idx);
            let col_metadata = row_group.column(col_idx);

            if let Some(stats) = col_metadata.statistics() {
                // Update min value (keep the smallest)
                if let Some(min_str) = parquet_stat_to_string(stats, true) {
                    if global_min.is_none() || min_str < global_min.as_ref().unwrap().clone() {
                        global_min = Some(min_str);
                    }
                }

                // Update max value (keep the largest)
                if let Some(max_str) = parquet_stat_to_string(stats, false) {
                    if global_max.is_none() || max_str > global_max.as_ref().unwrap().clone() {
                        global_max = Some(max_str);
                    }
                }

                // Accumulate null count
                if let Some(nc) = stats.null_count_opt() {
                    total_null_count += nc as usize;
                }

                // Try to get distinct count (not always available)
                if distinct_count.is_none() {
                    distinct_count = stats.distinct_count_opt().map(|d| d as usize);
                }
            }
        }

        column_statistics.push(optd_catalog::ColumnStatistics {
            column_id: 0, // External tables don't have column IDs
            column_type: col_type,
            name: col_name,
            advanced_stats: vec![],
            min_value: global_min,
            max_value: global_max,
            null_count: Some(total_null_count),
            distinct_count,
        });
    }

    Ok(column_statistics)
}

/// Converts Parquet statistics to string representation.
///
/// Returns min or max value as string. For optimizer integration, use proper ScalarValue.
fn parquet_stat_to_string(
    stats: &parquet::file::statistics::Statistics,
    is_min: bool,
) -> Option<String> {
    use parquet::file::statistics::Statistics;

    match stats {
        Statistics::Boolean(s) => {
            if is_min { s.min_opt() } else { s.max_opt() }.map(|v| v.to_string())
        }
        Statistics::Int32(s) => {
            if is_min { s.min_opt() } else { s.max_opt() }.map(|v| v.to_string())
        }
        Statistics::Int64(s) => {
            if is_min { s.min_opt() } else { s.max_opt() }.map(|v| v.to_string())
        }
        Statistics::Float(s) => {
            if is_min { s.min_opt() } else { s.max_opt() }.map(|v| v.to_string())
        }
        Statistics::Double(s) => {
            if is_min { s.min_opt() } else { s.max_opt() }.map(|v| v.to_string())
        }
        Statistics::ByteArray(s) => if is_min { s.min_opt() } else { s.max_opt() }
            .map(|v| String::from_utf8_lossy(v.data()).to_string()),
        Statistics::FixedLenByteArray(s) => if is_min { s.min_opt() } else { s.max_opt() }
            .map(|v| String::from_utf8_lossy(v.data()).to_string()),
        Statistics::Int96(_) => None, // Int96 is deprecated, skip
    }
}

/// Estimates statistics by sampling a CSV file.
///
/// Reads up to `sample_size` rows to estimate row count and column statistics.
async fn compute_csv_statistics(
    _location: &str,
    _sample_size: usize,
) -> Result<Option<TableStatistics>> {
    // TODO: Implement CSV sampling with configurable sample size
    Ok(None)
}

/// Estimates statistics by sampling a JSON file.
///
/// Reads up to `sample_size` rows to estimate row count and column statistics.
async fn compute_json_statistics(
    _location: &str,
    _sample_size: usize,
) -> Result<Option<TableStatistics>> {
    // TODO: Implement JSON sampling with configurable sample size
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AutoStatsConfig::default();
        assert!(config.enabled);
        assert!(config.parquet_enabled);
        assert!(!config.csv_enabled); // Disabled by default
        assert!(!config.json_enabled); // Disabled by default
        assert_eq!(config.sample_size, 10000);
    }

    #[test]
    fn test_is_enabled_for_format() {
        let config = AutoStatsConfig::default();
        assert!(config.is_enabled_for_format("PARQUET"));
        assert!(config.is_enabled_for_format("parquet"));
        assert!(!config.is_enabled_for_format("CSV"));
        assert!(!config.is_enabled_for_format("JSON"));
    }

    #[test]
    fn test_disabled_globally() {
        let config = AutoStatsConfig {
            enabled: false,
            ..Default::default()
        };
        assert!(!config.is_enabled_for_format("PARQUET"));
        assert!(!config.is_enabled_for_format("CSV"));
    }
}
