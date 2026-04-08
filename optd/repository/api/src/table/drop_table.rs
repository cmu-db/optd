use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, sea_query::Expr};

use crate::{
    entity::{column, prelude::*, table},
    snapshot::SnapshotInfo,
    table::DropTableInfo,
};

pub async fn drop_table<C>(
    info: DropTableInfo,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let end_snapshot = current_snapshot.snapshot_id;

    Table::update_many()
        .col_expr(table::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(table::Column::EndSnapshot.is_null())
        .filter(table::Column::TableId.eq(info.table_id))
        .exec(db)
        .await?;

    Column::update_many()
        .col_expr(column::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(column::Column::EndSnapshot.is_null())
        .filter(column::Column::TableId.eq(info.table_id))
        .exec(db)
        .await?;

    Ok(())
}
