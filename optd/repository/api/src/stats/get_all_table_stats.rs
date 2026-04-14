use std::collections::HashMap;

use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
};

use crate::{
    entity::{prelude::*, table_column_stats, table_stats},
    snapshot::SnapshotInfo,
    stats::{TableStatsInfo, build_table_stats_info},
};

pub async fn get_all_table_stats<C>(
    db: &C,
    current_snapshot: &SnapshotInfo,
) -> Result<Vec<TableStatsInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id;

    let table_stats = TableStats::find()
        .filter(table_stats::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table_stats::Column::EndSnapshot.is_null())
                .add(table_stats::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(table_stats::Column::TableId)
        .all(db)
        .await?;

    if table_stats.is_empty() {
        return Ok(Vec::new());
    }

    let table_ids = table_stats
        .iter()
        .map(|stats| stats.table_id)
        .collect::<Vec<_>>();
    let column_stats = TableColumnStats::find()
        .filter(table_column_stats::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table_column_stats::Column::EndSnapshot.is_null())
                .add(table_column_stats::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(table_column_stats::Column::TableId.is_in(table_ids))
        .order_by_asc(table_column_stats::Column::TableId)
        .order_by_asc(table_column_stats::Column::ColumnId)
        .all(db)
        .await?;

    let mut column_stats_by_table = HashMap::<i64, Vec<table_column_stats::Model>>::new();
    for column_stat in column_stats {
        column_stats_by_table
            .entry(column_stat.table_id)
            .or_default()
            .push(column_stat);
    }

    table_stats
        .into_iter()
        .map(|table_stat| {
            let table_id = table_stat.table_id;
            build_table_stats_info(
                table_stat,
                column_stats_by_table.remove(&table_id).unwrap_or_default(),
            )
        })
        .collect()
}
