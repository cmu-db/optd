use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter,
    QuerySelect,
};

use crate::{
    entity::{column, prelude::*, relation, schema},
    relation::{CreateColumnInfo, CreateRelationInfo},
};

pub(crate) async fn create_relation<C>(
    info: CreateRelationInfo,
    relation_id: i64,
    db: &C,
    read_snapshot: i64,
    begin_snapshot: i64,
) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let schema_name = &info.schema_name;
    let relation_name = &info.relation_name;

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

    let existing_relation_id = Relation::find()
        .filter(relation::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(relation::Column::EndSnapshot.is_null())
                .add(relation::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(relation::Column::SchemaId.eq(schema_id))
        .filter(relation::Column::RelationName.eq(relation_name))
        .select_only()
        .column(relation::Column::RelationId)
        .into_tuple::<i64>()
        .one(db)
        .await?;

    if existing_relation_id.is_some() {
        return Err(DbErr::Custom(format!(
            "Relation '{}.{}' already exists",
            schema_name, relation_name
        )));
    }

    let relation_model = relation::ActiveModel {
        relation_id: Set(relation_id),
        begin_snapshot: Set(begin_snapshot),
        end_snapshot: Set(None),
        schema_id: Set(schema_id),
        relation_name: Set(relation_name.to_owned()),
        relation_kind: Set(info.relation_kind),
        storage_kind: Set(info.storage_kind),
        ..Default::default()
    };

    let column_models = prepare_columns(info.columns, relation_id, begin_snapshot);

    Relation::insert(relation_model).exec(db).await?;

    if !column_models.is_empty() {
        Column::insert_many(column_models).exec(db).await?;
    }

    Ok(relation_id)
}

fn prepare_columns(
    columns: Vec<CreateColumnInfo>,
    relation_id: i64,
    begin_snapshot: i64,
) -> Vec<column::ActiveModel> {
    columns
        .into_iter()
        .enumerate()
        .map(|(i, column)| column::ActiveModel {
            relation_id: Set(relation_id),
            column_id: Set(i as i64),
            begin_snapshot: Set(begin_snapshot),
            end_snapshot: Set(None),
            column_order: Set(i as i64),
            column_name: Set(column.column_name),
            column_type: Set(column.column_type),
            nulls_allowed: Set(column.nulls_allowed),
            ..Default::default()
        })
        .collect()
}
