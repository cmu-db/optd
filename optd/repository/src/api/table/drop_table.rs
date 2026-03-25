use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, sea_query::Expr};

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

    let end_snapshot = current_snapshot.snapshot_id;

    Table::update_many()
        .col_expr(table::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(table::Column::EndSnapshot.is_null())
        .filter(table::Column::TableId.is_in(table_ids.iter().copied()))
        .exec(db)
        .await?;

    Column::update_many()
        .col_expr(column::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(column::Column::EndSnapshot.is_null())
        .filter(column::Column::TableId.is_in(table_ids.iter().copied()))
        .exec(db)
        .await?;

    Ok(())
}
