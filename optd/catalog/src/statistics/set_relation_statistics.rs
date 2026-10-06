use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
    prelude::ChronoUtc,
};

use crate::{
    CatalogError, CatalogResult,
    entity::{column_statistics, prelude::*, statistics, statistics_payload},
    relation::{GetRelationInfo, get_relation},
    snapshot::SnapshotInfo,
    statistics::{STATISTICS_VALUE_ENCODING_VERSION, SetRelationStatisticsInfo, StatisticsValue},
};

pub(crate) async fn get_statistics_definition<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    relation_id: i64,
) -> CatalogResult<Option<statistics::Model>> {
    let snapshot_id = snapshot.snapshot_id.as_i64();
    Ok(Statistics::find()
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
        .await?)
}

pub(crate) async fn insert_relation_statistics<C: ConnectionTrait>(
    db: &C,
    read_snapshot: &SnapshotInfo,
    new_snapshot_id: i64,
    statistics_id: i64,
    definition_begin_snapshot: i64,
    create_definition: bool,
    info: SetRelationStatisticsInfo,
) -> CatalogResult<()> {
    let relation = get_relation(
        GetRelationInfo {
            relation_id: info.relation_id,
        },
        db,
        read_snapshot,
    )
    .await?
    .ok_or(CatalogError::RelationNotFound {
        relation_id: info.relation_id,
    })?;
    let visible_column_ids = relation
        .columns
        .iter()
        .map(|column| column.column_id)
        .collect::<std::collections::BTreeSet<_>>();
    for column in &info.column_statistics {
        if !visible_column_ids.contains(&column.column_id) {
            return Err(CatalogError::StatisticsColumnNotFound {
                relation_id: info.relation_id,
                column_id: column.column_id,
            });
        }
    }

    if create_definition {
        Statistics::insert(statistics::ActiveModel {
            row_id: NotSet,
            statistics_id: Set(statistics_id),
            relation_id: Set(info.relation_id),
            statistics_name: Set(None),
            statistics_kind: Set("table".to_owned()),
            begin_snapshot: Set(definition_begin_snapshot),
            end_snapshot: Set(None),
        })
        .exec(db)
        .await?;
    }

    StatisticsPayload::insert(statistics_payload::ActiveModel {
        row_id: NotSet,
        statistics_id: Set(statistics_id),
        statistics_begin_snapshot: Set(definition_begin_snapshot),
        catalog_snapshot_id: Set(new_snapshot_id),
        data_snapshot_id: Set(info.data_snapshot_id),
        value_encoding_version: Set(STATISTICS_VALUE_ENCODING_VERSION),
        collected_at: Set(ChronoUtc::now()),
        row_count: Set(encode_count("row_count", info.row_count)?),
        size_bytes: Set(encode_count("size_bytes", info.size_bytes)?),
    })
    .exec(db)
    .await?;

    if !info.column_statistics.is_empty() {
        let columns = info
            .column_statistics
            .into_iter()
            .map(|column| {
                Ok(column_statistics::ActiveModel {
                    row_id: NotSet,
                    statistics_id: Set(statistics_id),
                    statistics_begin_snapshot: Set(definition_begin_snapshot),
                    catalog_snapshot_id: Set(new_snapshot_id),
                    column_id: Set(column.column_id),
                    frequency: Set(encode_count("frequency", column.frequency)?),
                    distinct_count: Set(encode_count("distinct_count", column.distinct_count)?),
                    lower_bound: Set(encode_value(column.lower_bound)?),
                    upper_bound: Set(encode_value(column.upper_bound)?),
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        ColumnStatistics::insert_many(columns).exec(db).await?;
    }

    Ok(())
}

fn encode_count(field: &'static str, value: Option<u64>) -> CatalogResult<Option<i64>> {
    value
        .map(|value| {
            i64::try_from(value).map_err(|_| CatalogError::StatisticsCountOutOfRange {
                field,
                value: value.to_string(),
            })
        })
        .transpose()
}

fn encode_value(value: Option<StatisticsValue>) -> CatalogResult<Option<String>> {
    value
        .map(|value| {
            serde_json::to_string(&value).map_err(|source| CatalogError::StatisticsValue { source })
        })
        .transpose()
}
