use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QuerySelect,
    sea_query::Expr,
};

use crate::{
    entity::{prelude::*, schema, table},
    schema::DropSchemaInfo,
};

pub async fn drop_schema<C>(
    info: DropSchemaInfo,
    db: &C,
    read_snapshot: i64,
    end_snapshot: i64,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if let Some((schema_id, table_name)) = Table::find()
        .filter(table::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(table::Column::EndSnapshot.is_null())
                .add(table::Column::EndSnapshot.gt(read_snapshot)),
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
        .col_expr(schema::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(schema::Column::EndSnapshot.is_null())
        .filter(schema::Column::SchemaId.eq(info.schema_id))
        .exec(db)
        .await?;

    Ok(())
}
