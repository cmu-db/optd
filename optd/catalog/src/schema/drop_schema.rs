use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, sea_query::Expr,
};

use crate::{
    CatalogError, CatalogResult,
    entity::{prelude::*, relation, schema},
    schema::DropSchemaInfo,
};

pub async fn drop_schema<C>(
    info: DropSchemaInfo,
    db: &C,
    read_snapshot: i64,
    end_snapshot: i64,
) -> CatalogResult<()>
where
    C: ConnectionTrait,
{
    if let Some((schema_id, relation_name)) = Relation::find()
        .filter(relation::Column::BeginSnapshot.lte(read_snapshot))
        .filter(
            Condition::any()
                .add(relation::Column::EndSnapshot.is_null())
                .add(relation::Column::EndSnapshot.gt(read_snapshot)),
        )
        .filter(relation::Column::SchemaId.eq(info.schema_id))
        .select_only()
        .column(relation::Column::SchemaId)
        .column(relation::Column::RelationName)
        .into_tuple::<(i64, String)>()
        .one(db)
        .await?
    {
        return Err(CatalogError::SchemaNotEmpty {
            schema_id,
            relation_name,
        });
    }

    Schema::update_many()
        .col_expr(schema::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(schema::Column::EndSnapshot.is_null())
        .filter(schema::Column::SchemaId.eq(info.schema_id))
        .exec(db)
        .await?;

    Ok(())
}
