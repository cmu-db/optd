use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    entity::{prelude::*, schema},
    schema::CreateSchemaInfo,
};

pub async fn create_new_schema<C>(
    info: CreateSchemaInfo,
    schema_id: i64,
    db: &C,
    begin_snapshot: i64,
) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let model = schema::ActiveModel {
        schema_id: Set(schema_id),
        begin_snapshot: Set(begin_snapshot),
        end_snapshot: Set(None),
        schema_name: Set(info.schema_name),
        ..Default::default()
    };

    Schema::insert(model).exec(db).await?;
    Ok(schema_id)
}
