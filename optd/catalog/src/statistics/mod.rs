mod get_relation_statistics;
mod set_relation_statistics;

pub(crate) use get_relation_statistics::*;
pub(crate) use set_relation_statistics::*;

use serde::{Deserialize, Serialize};

use crate::snapshot::SnapshotId;

/// Encoding version used for persisted [`StatisticsValue`] JSON payloads.
pub const STATISTICS_VALUE_ENCODING_VERSION: i32 = 1;

/// Scalar bound stored in a collected column-statistics payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StatisticsValue {
    /// Boolean value.
    Boolean(bool),
    /// Signed 32-bit integer value.
    Int32(i32),
    /// Signed 64-bit integer value.
    Int64(i64),
    /// IEEE 754 double-precision value.
    Float64(f64),
    /// UTF-8 string value.
    Utf8(String),
    /// Number of days since the Unix epoch.
    Date32(i32),
    /// Decimal value represented by an unscaled integer and its Arrow type parameters.
    Decimal128 {
        /// Unscaled decimal value.
        value: i128,
        /// Total number of decimal digits.
        precision: u8,
        /// Number of digits after the decimal point.
        scale: i8,
    },
}

/// Collected statistics for one durable relation column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    /// Relation-scoped durable column identifier.
    pub column_id: i64,
    /// Number of non-null values in the collected population.
    pub frequency: Option<u64>,
    /// Number of distinct non-null values.
    pub distinct_count: Option<u64>,
    /// Smallest observed non-null value.
    pub lower_bound: Option<StatisticsValue>,
    /// Largest observed non-null value.
    pub upper_bound: Option<StatisticsValue>,
}

/// Statistics payload to publish for a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct SetRelationStatisticsInfo {
    /// Stable identifier of the relation described by the payload.
    pub relation_id: i64,
    /// Number of rows in the collected population.
    pub row_count: Option<u64>,
    /// Physical size of the collected population in bytes.
    pub size_bytes: Option<u64>,
    /// Per-column statistics keyed by durable column ID.
    pub column_statistics: Vec<ColumnStatistics>,
}

/// Statistics visible for a relation at a catalog snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationStatistics {
    /// Stable identity of the statistics definition across payload refreshes.
    pub statistics_id: i64,
    /// Stable identifier of the relation described by the payload.
    pub relation_id: i64,
    /// First catalog snapshot in which this payload is visible.
    pub begin_snapshot: SnapshotId,
    /// First catalog snapshot in which this payload is no longer visible.
    pub end_snapshot: Option<SnapshotId>,
    /// Encoding version used by the typed column bounds in this payload.
    pub value_encoding_version: i32,
    /// Number of rows in the collected population.
    pub row_count: Option<u64>,
    /// Physical size of the collected population in bytes.
    pub size_bytes: Option<u64>,
    /// Per-column statistics keyed by durable column ID.
    pub column_statistics: Vec<ColumnStatistics>,
}
