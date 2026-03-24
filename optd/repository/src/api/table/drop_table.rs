use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QuerySelect, sea_query::Expr,
};

use crate::{
    api::snapshot::SnapshotInfo,
    entity::{column, prelude::*, table},
};

pub async fn drop_tables<C>(
    table_ids: &[i64],
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if table_ids.is_empty() {
        return Ok(());
    }

    let active_table_ids = Table::find()
        .select_only()
        .column(table::Column::TableId)
        .filter(table::Column::EndSnapshot.is_null())
        .filter(table::Column::TableId.is_in(table_ids.iter().copied()))
        .into_tuple::<i64>()
        .all(db)
        .await?;

    if active_table_ids.is_empty() {
        return Ok(());
    }

    let end_snapshot = current_snapshot.snapshot_id;

    Table::update_many()
        .col_expr(table::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(table::Column::EndSnapshot.is_null())
        .filter(table::Column::TableId.is_in(active_table_ids.iter().copied()))
        .exec(db)
        .await?;

    Column::update_many()
        .col_expr(column::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(column::Column::EndSnapshot.is_null())
        .filter(column::Column::TableId.is_in(active_table_ids.into_iter()))
        .exec(db)
        .await?;

    Ok(())
}
