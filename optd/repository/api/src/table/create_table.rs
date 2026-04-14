use optd_core::ir::table_ref::TableRef;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter,
    QuerySelect, prelude::Uuid,
};

use crate::{
    entity::{column, prelude::*, schema, table},
    table::{CreateColumnInfo, CreateTableInfo},
};

pub async fn create_table<C>(
    info: CreateTableInfo,
    table_id: i64,
    db: &C,
    read_snapshot: i64,
    begin_snapshot: i64,
) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let schema_name = target_schema_name(&info.table_name);
    let table_name = info.table_name.table();

    let schema_id = Schema::find()
        .filter(schema::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(schema::Column::EndSnapshot.is_null())
                .add(schema::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(schema::Column::SchemaName.eq(schema_name))
        .select_only()
        .column(schema::Column::SchemaId)
        .into_tuple::<i64>()
        .one(db)
        .await?
        .ok_or_else(|| {
            DbErr::Custom(format!(
                "Schema '{schema_name}' does not exist at snapshot {read_snapshot}"
            ))
        })?;

    let existing_table_id = Table::find()
        .filter(table::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(table::Column::EndSnapshot.is_null())
                .add(table::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(table::Column::SchemaId.eq(schema_id))
        .filter(table::Column::TableName.eq(table_name))
        .select_only()
        .column(table::Column::TableId)
        .into_tuple::<i64>()
        .one(db)
        .await?;

    if existing_table_id.is_some() {
        return Err(DbErr::Custom(format!(
            "Table '{}.{}' already exists",
            schema_name, table_name
        )));
    }

    let table_model = table::ActiveModel {
        table_id: Set(table_id),
        table_uuid: Set(Uuid::new_v4()),
        begin_snapshot: Set(begin_snapshot),
        end_snapshot: Set(None),
        schema_id: Set(schema_id),
        table_name: Set(table_name.to_owned()),
        definition: Set(info.definition),
        ..Default::default()
    };

    let column_models = prepare_columns(info.columns, table_id, begin_snapshot);

    Table::insert(table_model).exec(db).await?;

    if !column_models.is_empty() {
        Column::insert_many(column_models).exec(db).await?;
    }

    Ok(table_id)
}

fn prepare_columns(
    columns: Vec<CreateColumnInfo>,
    table_id: i64,
    begin_snapshot: i64,
) -> Vec<column::ActiveModel> {
    columns
        .into_iter()
        .enumerate()
        .map(|(i, column)| column::ActiveModel {
            table_id: Set(table_id),
            column_id: Set(i as i64),
            begin_snapshot: Set(begin_snapshot),
            end_snapshot: Set(None),
            column_order: Set(i as i64),
            column_name: Set(column.column_name),
            column_type: Set(column.column_type),
            initial_default: Set(column.initial_default),
            default_value: Set(column.default_value),
            nulls_allowed: Set(column.nulls_allowed),
            parent_column: Set(None),
            ..Default::default()
        })
        .collect()
}

/// Extract the schema name from table reference, otherwise use `public` as the default.
fn target_schema_name(table_ref: &TableRef) -> &str {
    table_ref.schema().unwrap_or("public")
}
