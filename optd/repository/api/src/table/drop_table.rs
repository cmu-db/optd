use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, sea_query::Expr};

use crate::{
    entity::{column, prelude::*, table, table_column_stats, table_stats},
    table::DropTableInfo,
};

pub async fn drop_table<C>(info: DropTableInfo, db: &C, end_snapshot: i64) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
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

    TableStats::update_many()
        .col_expr(table_stats::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(table_stats::Column::EndSnapshot.is_null())
        .filter(table_stats::Column::TableId.eq(info.table_id))
        .exec(db)
        .await?;

    TableColumnStats::update_many()
        .col_expr(
            table_column_stats::Column::EndSnapshot,
            Expr::value(end_snapshot),
        )
        .filter(table_column_stats::Column::EndSnapshot.is_null())
        .filter(table_column_stats::Column::TableId.eq(info.table_id))
        .exec(db)
        .await?;

    Ok(())
}
