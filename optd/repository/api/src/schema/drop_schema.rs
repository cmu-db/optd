use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QuerySelect,
    sea_query::Expr,
};

use crate::{
    schema::DropSchemaInfo,
    snapshot::SnapshotInfo,
    entity::{prelude::*, schema, table},
};

pub async fn drop_schema<C>(
    info: DropSchemaInfo,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id;

    if let Some((schema_id, table_name)) = Table::find()
        .filter(table::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(table::Column::EndSnapshot.is_null())
                .add(table::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(table::Column::SchemaId.eq(info.schema_id))
        .select_only()
        .column(table::Column::SchemaId)
        .column(table::Column::TableName)
        .into_tuple::<(i64, String)>()
        .one(db)
        .await?
    {
        return Err(DbErr::Custom(format!(
            "Cannot drop schema {schema_id}: active table '{table_name}' still exists"
        )));
    }

    Schema::update_many()
        .col_expr(schema::Column::EndSnapshot, Expr::value(snapshot_id))
        .filter(schema::Column::EndSnapshot.is_null())
        .filter(schema::Column::SchemaId.eq(info.schema_id))
        .exec(db)
        .await?;

    Ok(())
}
