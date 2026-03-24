use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    api::{
        snapshot::SnapshotInfo,
        table::{ColumnInfo, TableInfo},
    },
    entity::{column, prelude::*, table},
};

pub async fn create_table<C>(
    schema_id: i64,
    table_name: &str,
    columns: &[ColumnInfo],
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<TableInfo, DbErr>
where
    C: ConnectionTrait,
{
    let table_id = current_snapshot.get_next_catalog_id();
    let table_uuid = uuid::Uuid::new_v4();

    let model = table::ActiveModel {
        table_id: Set(table_id),
        table_uuid: Set(table_uuid),
        begin_snapshot: Set(current_snapshot.snapshot_id),
        end_snapshot: Set(None),
        schema_id: Set(schema_id),
        table_name: Set(table_name.to_owned()),
        ..Default::default()
    };

    Table::insert(model).exec(db).await?;

    let mut column_models = Vec::new();
    let mut next_column_order = 0;
    let created_columns = normalize_columns(
        columns,
        table_id,
        None,
        current_snapshot,
        &mut next_column_order,
        &mut column_models,
    );

    if !column_models.is_empty() {
        Column::insert_many(column_models).exec(db).await?;
    }

    Ok(TableInfo {
        id: table_id,
        schema_id,
        table_uuid,
        table_name: table_name.to_owned(),
        columns: created_columns,
    })
}

fn normalize_columns(
    columns: &[ColumnInfo],
    table_id: i64,
    parent_column: Option<i64>,
    current_snapshot: &mut SnapshotInfo,
    next_column_order: &mut i64,
    column_models: &mut Vec<column::ActiveModel>,
) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|column| {
            let column_id = if column.id > 0 {
                column.id
            } else {
                current_snapshot.get_next_catalog_id()
            };

            column_models.push(column::ActiveModel {
                table_id: Set(table_id),
                column_id: Set(column_id),
                begin_snapshot: Set(current_snapshot.snapshot_id),
                end_snapshot: Set(None),
                column_order: Set(*next_column_order),
                column_name: Set(column.column_name.clone()),
                column_type: Set(column.column_type.clone()),
                initial_default: Set(column.initial_default.clone()),
                default_value: Set(column.default_value.clone()),
                nulls_allowed: Set(column.nulls_allowed),
                parent_column: Set(parent_column),
                ..Default::default()
            });
            *next_column_order += 1;

            let children = normalize_columns(
                &column.children,
                table_id,
                Some(column_id),
                current_snapshot,
                next_column_order,
                column_models,
            );

            ColumnInfo {
                id: column_id,
                column_name: column.column_name.clone(),
                column_type: column.column_type.clone(),
                initial_default: column.initial_default.clone(),
                default_value: column.default_value.clone(),
                nulls_allowed: column.nulls_allowed,
                children,
            }
        })
        .collect()
}
