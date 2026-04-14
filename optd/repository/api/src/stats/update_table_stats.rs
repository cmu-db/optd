use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter,
    QuerySelect, sea_query::Expr,
};

use crate::{
    entity::{column, prelude::*, table, table_column_stats, table_stats},
    stats::UpdateTableStatsInfo,
};

pub async fn update_table_stats<C>(
    info: UpdateTableStatsInfo,
    db: &C,
    read_snapshot: i64,
    write_snapshot: i64,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let table_exists = Table::find()
        .filter(table::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(table::Column::EndSnapshot.is_null())
                .add(table::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(table::Column::TableId.eq(info.table_id))
        .select_only()
        .column(table::Column::TableId)
        .into_tuple::<i64>()
        .one(db)
        .await?
        .is_some();

    if !table_exists {
        return Err(DbErr::RecordNotFound(format!(
            "table {} not found",
            info.table_id
        )));
    }

    let active_columns = Column::find()
        .filter(column::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(column::Column::EndSnapshot.is_null())
                .add(column::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(column::Column::TableId.eq(info.table_id))
        .select_only()
        .column(column::Column::ColumnId)
        .into_tuple::<i64>()
        .all(db)
        .await?;

    let active_column_ids = active_columns
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    for column_id in info.stats.column_statistics.keys() {
        let column_id = *column_id as i64;
        if !active_column_ids.contains(&column_id) {
            return Err(DbErr::RecordNotFound(format!(
                "column {column_id} not found for table {}",
                info.table_id
            )));
        }
    }

    let next_row_id = TableStats::find()
        .filter(table_stats::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(table_stats::Column::EndSnapshot.is_null())
                .add(table_stats::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(table_stats::Column::TableId.eq(info.table_id))
        .one(db)
        .await?
        .map(|existing| existing.next_row_id)
        .unwrap_or(info.stats.row_count as i64);

    TableStats::update_many()
        .col_expr(
            table_stats::Column::EndSnapshot,
            Expr::value(write_snapshot),
        )
        .filter(table_stats::Column::EndSnapshot.is_null())
        .filter(table_stats::Column::TableId.eq(info.table_id))
        .exec(db)
        .await?;

    TableColumnStats::update_many()
        .col_expr(
            table_column_stats::Column::EndSnapshot,
            Expr::value(write_snapshot),
        )
        .filter(table_column_stats::Column::EndSnapshot.is_null())
        .filter(table_column_stats::Column::TableId.eq(info.table_id))
        .exec(db)
        .await?;

    let table_stats_model = table_stats::ActiveModel {
        table_id: Set(info.table_id),
        begin_snapshot: Set(write_snapshot),
        end_snapshot: Set(None),
        record_count: Set(info.stats.row_count as i64),
        next_row_id: Set(next_row_id),
        file_size_bytes: Set(info.stats.size_bytes.unwrap_or_default() as i64),
        ..Default::default()
    };
    TableStats::insert(table_stats_model).exec(db).await?;

    let column_stats_models = info
        .stats
        .column_statistics
        .into_iter()
        .map(
            |(column_id, column_stats)| table_column_stats::ActiveModel {
                table_id: Set(info.table_id),
                column_id: Set(column_id as i64),
                begin_snapshot: Set(write_snapshot),
                end_snapshot: Set(None),
                contains_null: Set(column_stats.null_count.map(|n| n > 0).unwrap_or(true)),
                contains_nan: Set(true),
                min_value: Set(column_stats.min_value),
                max_value: Set(column_stats.max_value),
                distinct_count: Set(column_stats.distinct_count.map(|v| v as i64)),
                null_count: Set(column_stats.null_count.map(|v| v as i64)),
                ..Default::default()
            },
        )
        .collect::<Vec<_>>();

    if !column_stats_models.is_empty() {
        TableColumnStats::insert_many(column_stats_models)
            .exec(db)
            .await?;
    }

    Ok(())
}
