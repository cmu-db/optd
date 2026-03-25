use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, prelude::Uuid};

use crate::{
    api::{
        snapshot::SnapshotInfo,
        table::{ColumnInfo, TableInfo},
    },
    entity::{column, prelude::*, table},
};

pub async fn create_tables<C>(
    tables: &[TableInfo],
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if tables.is_empty() {
        return Ok(());
    }

    let PreparedTables {
        table_models,
        column_models,
    } = prepare_tables(tables, current_snapshot);

    Table::insert_many(table_models)
        .exec_without_returning(db)
        .await?;

    if !column_models.is_empty() {
        Column::insert_many(column_models)
            .exec_without_returning(db)
            .await?;
    }

    Ok(())
}

struct PreparedTables {
    table_models: Vec<table::ActiveModel>,
    column_models: Vec<column::ActiveModel>,
}

fn prepare_tables(tables: &[TableInfo], current_snapshot: &mut SnapshotInfo) -> PreparedTables {
    let mut table_models = Vec::with_capacity(tables.len());
    let mut column_models = Vec::new();

    for table in tables {
        let table_id = if table.table_id > 0 {
            table.table_id
        } else {
            current_snapshot.get_next_catalog_id()
        };
        let table_uuid = if table.table_uuid.is_nil() {
            Uuid::new_v4()
        } else {
            table.table_uuid
        };

        table_models.push(table::ActiveModel {
            table_id: Set(table_id),
            table_uuid: Set(table_uuid),
            begin_snapshot: Set(current_snapshot.snapshot_id),
            end_snapshot: Set(None),
            schema_id: Set(table.schema_id),
            table_name: Set(table.table_name.clone()),
            ..Default::default()
        });

        let mut next_column_order = 0;
        let _columns = normalize_columns(
            &table.columns,
            table_id,
            None,
            current_snapshot,
            &mut next_column_order,
            &mut column_models,
        );
    }

    PreparedTables {
        table_models,
        column_models,
    }
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
            let column_id = if column.column_id > 0 {
                column.column_id
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
                column_id,
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
