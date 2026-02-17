use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Table statistics (row count + column stats)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableStatistics {
    pub row_count: usize,
    pub column_statistics: Vec<ColumnStatistics>,

    /// File size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<usize>,
}

/// Column statistics (external tables use column_id=0, name for identification)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnStatistics {
    pub column_id: i64,
    pub column_type: String,
    pub name: String,
    pub advanced_stats: Vec<AdvanceColumnStatistics>,

    /// TODO(Aditya): Move this to Value?
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
    pub fn new(
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

    pub fn add_advanced_stat(&mut self, stat: AdvanceColumnStatistics) {
        self.advanced_stats.push(stat);
    }
}

/// An advanced statistics entry with type and serialized data at a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdvanceColumnStatistics {
    /// Type of the statistical summaries (e.g., histogram, distinct count).
    pub stats_type: String,
    /// Serialized data for the statistics at a snapshot.
    pub data: Value,
}
