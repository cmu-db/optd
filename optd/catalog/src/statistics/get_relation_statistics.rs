use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};

use crate::{
    CatalogError, CatalogResult,
    entity::{column_statistics, prelude::*, statistics, statistics_payload},
    relation::{GetRelationInfo, get_relation},
    snapshot::{SnapshotId, SnapshotInfo},
    statistics::{
        ColumnStatistics as ColumnStatisticsInfo, RelationStatistics,
        STATISTICS_VALUE_ENCODING_VERSION, StatisticsValue, decode_column_ids,
    },
};

pub(crate) async fn get_relation_statistics<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    relation_id: i64,
) -> CatalogResult<Option<RelationStatistics>> {
    if get_relation(GetRelationInfo { relation_id }, db, snapshot)
        .await?
        .is_none()
    {
        return Ok(None);
    }

    let snapshot_id = snapshot.snapshot_id.as_i64();
    let Some(definition) = Statistics::find()
        .filter(statistics::Column::RelationId.eq(relation_id))
        .filter(statistics::Column::StatisticsKind.eq("table"))
        .filter(statistics::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(statistics::Column::EndSnapshot.is_null())
                .add(statistics::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_desc(statistics::Column::BeginSnapshot)
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    let Some(payload) = StatisticsPayload::find()
        .filter(statistics_payload::Column::StatisticsId.eq(definition.statistics_id))
        .filter(statistics_payload::Column::StatisticsBeginSnapshot.eq(definition.begin_snapshot))
        .filter(statistics_payload::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(statistics_payload::Column::EndSnapshot.is_null())
                .add(statistics_payload::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_desc(statistics_payload::Column::BeginSnapshot)
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    if payload.value_encoding_version != STATISTICS_VALUE_ENCODING_VERSION {
        return Err(CatalogError::UnsupportedStatisticsValueEncoding {
            version: payload.value_encoding_version,
        });
    }

    let columns = ColumnStatistics::find()
        .filter(column_statistics::Column::StatisticsId.eq(definition.statistics_id))
        .filter(column_statistics::Column::StatisticsBeginSnapshot.eq(definition.begin_snapshot))
        .filter(column_statistics::Column::PayloadBeginSnapshot.eq(payload.begin_snapshot))
        .order_by_asc(column_statistics::Column::ColumnId)
        .all(db)
        .await?
        .into_iter()
        .map(|column| {
            Ok(ColumnStatisticsInfo {
                column_id: column.column_id,
                frequency: decode_count("frequency", column.frequency)?,
                distinct_count: decode_count("distinct_count", column.distinct_count)?,
                lower_bound: decode_value(column.lower_bound)?,
                upper_bound: decode_value(column.upper_bound)?,
            })
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    if let Some(pair) = columns
        .windows(2)
        .find(|pair| pair[0].column_id == pair[1].column_id)
    {
        return Err(CatalogError::DuplicateColumnStatistics {
            relation_id,
            column_id: pair[0].column_id,
        });
    }

    Ok(Some(RelationStatistics {
        statistics_id: definition.statistics_id,
        relation_id,
        column_ids: decode_column_ids(definition.column_ids)?,
        begin_snapshot: SnapshotId::from_raw(payload.begin_snapshot),
        end_snapshot: payload.end_snapshot.map(SnapshotId::from_raw),
        value_encoding_version: payload.value_encoding_version,
        row_count: decode_count("row_count", payload.row_count)?,
        size_bytes: decode_count("size_bytes", payload.size_bytes)?,
        column_statistics: columns,
    }))
}

fn decode_count(field: &'static str, value: Option<i64>) -> CatalogResult<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| CatalogError::StatisticsCountOutOfRange {
                field,
                value: value.to_string(),
            })
        })
        .transpose()
}

fn decode_value(value: Option<String>) -> CatalogResult<Option<StatisticsValue>> {
    // The v1 writer previously serialized NaN and infinities as {"Float64":null}.
    // Recover exactly that lossy shape as unknown, not every malformed bound. The
    // typed helper rejects extra/duplicate fields, and avoids decoding valid i128
    // decimals through serde_json::Value (which could lose integer precision).
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LegacyNonFiniteFloat {
        #[serde(rename = "Float64")]
        _value: (),
    }

    value
        .map(|value| {
            if serde_json::from_str::<LegacyNonFiniteFloat>(&value).is_ok() {
                return Ok(None);
            }
            serde_json::from_str::<StatisticsValue>(&value)
                .map(Some)
                .map_err(|source| CatalogError::StatisticsValue { source })
        })
        .transpose()
        .map(Option::flatten)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_non_finite_statistics_bounds_are_unknown() {
        for encoded in ["{\"Float64\":null}", "{ \"Float64\" : null }"] {
            assert_eq!(decode_value(Some(encoded.to_owned())).unwrap(), None);
        }
        assert_eq!(decode_value(None).unwrap(), None);
    }

    #[test]
    fn finite_statistics_bounds_round_trip_and_malformed_bounds_still_fail() {
        for value in [f64::MIN, -0.0, 0.0, 1.5, f64::MAX] {
            let encoded = serde_json::to_string(&StatisticsValue::Float64(value)).unwrap();
            let Some(StatisticsValue::Float64(decoded)) = decode_value(Some(encoded)).unwrap()
            else {
                panic!("finite Float64 must be preserved");
            };
            assert_eq!(value.to_bits(), decoded.to_bits());
        }
        assert_eq!(
            decode_value(Some("{\"Int64\":42}".into())).unwrap(),
            Some(StatisticsValue::Int64(42))
        );
        let decimal = StatisticsValue::Decimal128 {
            value: 10_i128.pow(37) + 1,
            precision: 38,
            scale: 0,
        };
        assert_eq!(
            decode_value(Some(serde_json::to_string(&decimal).unwrap())).unwrap(),
            Some(decimal)
        );
        for encoded in [
            "broken",
            "{}",
            "null",
            "{\"Int64\":null}",
            "{\"Float64\":\"Infinity\"}",
            "{\"Float64\":null,\"extra\":1}",
            "{\"Float64\":1.0,\"Float64\":null}",
        ] {
            assert!(decode_value(Some(encoded.to_owned())).is_err(), "{encoded}");
        }
    }
}
