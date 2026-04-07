use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
};

use crate::{
    snapshot::SnapshotInfo,
    table::{ColumnInfo, GetTableInfo, TableInfo},
    entity::{column, prelude::*, table},
};

pub async fn get_table<C>(
    info: GetTableInfo,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<Option<TableInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id;

    let Some(table) = Table::find()
        .filter(table::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table::Column::EndSnapshot.is_null())
                .add(table::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(table::Column::TableId.eq(info.table_id))
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    let columns = Column::find()
        .filter(column::Column::TableId.eq(table.table_id))
        .filter(column::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(column::Column::EndSnapshot.is_null())
                .add(column::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(column::Column::ColumnOrder)
        .all(db)
        .await?;

    Ok(Some(TableInfo {
        table_id: table.table_id,
        schema_id: table.schema_id,
        table_uuid: table.table_uuid,
        table_name: table.table_name,
        columns: build_columns(columns)?,
    }))
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
                column_id: column.column_id as u64,
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
