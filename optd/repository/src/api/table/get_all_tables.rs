use std::collections::HashMap;

use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
};

use crate::{
    api::{
        snapshot::SnapshotInfo,
        table::{ColumnInfo, TableInfo},
    },
    entity::{column, prelude::*, table},
};

pub async fn get_all_table_infos<C>(
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<Vec<TableInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id;
    let tables = Table::find()
        .filter(table::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table::Column::EndSnapshot.is_null())
                .add(table::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(table::Column::TableId)
        .all(db)
        .await?;

    if tables.is_empty() {
        return Ok(Vec::new());
    }

    let table_ids = tables
        .iter()
        .map(|table| table.table_id)
        .collect::<Vec<_>>();
    let columns = Column::find()
        .filter(column::Column::TableId.is_in(table_ids))
        .filter(column::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(column::Column::EndSnapshot.is_null())
                .add(column::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(column::Column::TableId)
        .order_by_asc(column::Column::ColumnOrder)
        .all(db)
        .await?;

    let mut columns_by_table = HashMap::<i64, Vec<column::Model>>::new();
    for column in columns {
        columns_by_table
            .entry(column.table_id)
            .or_default()
            .push(column);
    }

    Ok(tables
        .into_iter()
        .map(|table| {
            Ok(TableInfo {
                table_id: table.table_id,
                schema_id: table.schema_id,
                table_uuid: table.table_uuid,
                table_name: table.table_name,
                columns: build_columns(
                    columns_by_table.remove(&table.table_id).unwrap_or_default(),
                )?,
            })
        })
        .collect::<Result<Vec<_>, DbErr>>()?)
}

fn build_columns(columns: Vec<column::Model>) -> Result<Vec<ColumnInfo>, DbErr> {
    Ok(columns
        .into_iter()
        .map(|column| {
            if column.parent_column.is_some() {
                return Err(DbErr::Custom(format!(
                    "Nested columns are not supported for table_id {}",
                    column.table_id
                )));
            }

            Ok(ColumnInfo {
                column_id: column.column_id,
                column_name: column.column_name,
                column_type: column.column_type,
                nulls_allowed: column.nulls_allowed,
                initial_default: column.initial_default,
                default_value: column.default_value,
                children: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>, DbErr>>()?)
}
