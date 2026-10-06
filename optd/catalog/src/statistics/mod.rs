mod get_relation_statistics;
mod set_relation_statistics;

pub(crate) use get_relation_statistics::*;
pub(crate) use set_relation_statistics::*;

use serde::{Deserialize, Serialize};

use crate::{CatalogError, CatalogResult, snapshot::SnapshotId};

/// Encoding version used for persisted [`StatisticsValue`] JSON payloads.
pub const STATISTICS_VALUE_ENCODING_VERSION: i32 = 1;

pub(crate) fn encode_column_ids(mut column_ids: Vec<i64>) -> CatalogResult<serde_json::Value> {
    column_ids.sort_unstable();
    if column_ids.iter().any(|column_id| *column_id < 0) {
        return Err(CatalogError::StatisticsColumnIds {
            reason: "column IDs must be non-negative".to_owned(),
        });
    }
    if column_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CatalogError::StatisticsColumnIds {
            reason: "column IDs must not contain duplicates".to_owned(),
        });
    }
    serde_json::to_value(column_ids).map_err(|source| CatalogError::StatisticsColumnIds {
        reason: source.to_string(),
    })
}

pub(crate) fn decode_column_ids(value: serde_json::Value) -> CatalogResult<Vec<i64>> {
    let column_ids: Vec<i64> =
        serde_json::from_value(value).map_err(|source| CatalogError::StatisticsColumnIds {
            reason: source.to_string(),
        })?;
    if column_ids.iter().any(|column_id| *column_id < 0) {
        return Err(CatalogError::StatisticsColumnIds {
            reason: "column IDs must be non-negative".to_owned(),
        });
    }
    if column_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CatalogError::StatisticsColumnIds {
            reason: "column IDs must be sorted and unique".to_owned(),
        });
    }
    Ok(column_ids)
}

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
    /// Canonically sorted durable column IDs targeted by the definition.
    ///
    /// The implicit table-statistics definition uses an empty vector.
    pub column_ids: Vec<i64>,
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

#[cfg(test)]
mod tests {
    use super::{decode_column_ids, encode_column_ids};

    #[test]
    fn statistics_column_ids_use_canonical_json_order() {
        let encoded = encode_column_ids(vec![9, 3, 7]).unwrap();
        assert_eq!(encoded, serde_json::json!([3, 7, 9]));
        assert_eq!(decode_column_ids(encoded).unwrap(), vec![3, 7, 9]);
    }

    #[test]
    fn statistics_column_ids_reject_duplicates_and_noncanonical_json() {
        assert!(encode_column_ids(vec![3, 3]).is_err());
        assert!(decode_column_ids(serde_json::json!([7, 3])).is_err());
        assert!(decode_column_ids(serde_json::json!([3, 3])).is_err());
    }
}
