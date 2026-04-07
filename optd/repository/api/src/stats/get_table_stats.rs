use optd_core::ir::statistics::{ColumnStatistics, TableStatistics};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
};

use crate::{
    entity::{prelude::*, table_column_stats, table_stats},
    snapshot::SnapshotInfo,
    stats::{GetTableStatsInfo, TableStatsInfo},
};

pub async fn get_table_stats<C>(
    info: GetTableStatsInfo,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<Option<TableStatsInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id;

    let Some(table_stats) = TableStats::find()
        .filter(table_stats::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table_stats::Column::EndSnapshot.is_null())
                .add(table_stats::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(table_stats::Column::TableId.eq(info.table_id))
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    let column_stats = TableColumnStats::find()
        .filter(table_column_stats::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table_column_stats::Column::EndSnapshot.is_null())
                .add(table_column_stats::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(table_column_stats::Column::TableId.eq(info.table_id))
        .order_by_asc(table_column_stats::Column::ColumnId)
        .all(db)
        .await?;

    Ok(Some(build_table_stats_info(table_stats, column_stats)?))
}

pub(crate) fn build_table_stats_info(
    table_stats: table_stats::Model,
    column_stats: Vec<table_column_stats::Model>,
) -> Result<TableStatsInfo, DbErr> {
    Ok(TableStatsInfo {
        table_id: table_stats.table_id,
        stats: TableStatistics {
            row_count: table_stats.record_count as u64 as usize,
            size_bytes: Some(table_stats.file_size_bytes as u64 as usize),
            column_statistics: column_stats
                .into_iter()
                .map(|column_stat| {
                    (
                        column_stat.column_id as u64 as usize,
                        ColumnStatistics {
                            min_value: column_stat.min_value,
                            max_value: column_stat.max_value,
                            null_count: column_stat.null_count.map(|v| v as u64 as usize),
                            distinct_count: column_stat
                                .distinct_count
                                .map(|value| value as u64 as usize),
                            advanced_stats: vec![],
                        },
                    )
                })
                .collect(),
        },
    })
}
