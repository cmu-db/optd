use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};

use crate::{
    CatalogError, CatalogResult,
    entity::{column_statistics, prelude::*, statistics, statistics_payload},
    relation::{GetRelationInfo, get_relation},
    snapshot::{SnapshotId, SnapshotInfo},
    statistics::{
        ColumnStatistics as ColumnStatisticsInfo, RelationStatistics,
        STATISTICS_VALUE_ENCODING_VERSION, StatisticsValue,
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

    Ok(Some(RelationStatistics {
        statistics_id: definition.statistics_id,
        relation_id,
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
    value
        .map(|value| {
            serde_json::from_str(&value).map_err(|source| CatalogError::StatisticsValue { source })
        })
        .transpose()
}
