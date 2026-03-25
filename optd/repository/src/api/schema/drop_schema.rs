use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QuerySelect, sea_query::Expr,
};

use crate::{
    api::snapshot::SnapshotInfo,
    entity::{prelude::*, schema, table},
};

pub async fn drop_schemas<C>(
    schema_ids: &[i64],
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if schema_ids.is_empty() {
        return Ok(());
    }

    if let Some((schema_id, table_name)) = Table::find()
        .select_only()
        .column(table::Column::SchemaId)
        .column(table::Column::TableName)
        .filter(table::Column::EndSnapshot.is_null())
        .filter(table::Column::SchemaId.is_in(schema_ids.iter().copied()))
        .into_tuple::<(i64, String)>()
        .one(db)
        .await?
    {
        return Err(DbErr::Custom(format!(
            "Cannot drop schema {schema_id}: active table '{table_name}' still exists"
        )));
    }

    let end_snapshot = current_snapshot.snapshot_id;

    Schema::update_many()
        .col_expr(schema::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(schema::Column::EndSnapshot.is_null())
        .filter(schema::Column::SchemaId.is_in(schema_ids.iter().copied()))
        .exec(db)
        .await?;

    Ok(())
}
