use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    schema::CreateSchemaInfo,
    snapshot::SnapshotInfo,
    entity::{prelude::*, schema},
};

pub async fn create_new_schema<C>(
    info: CreateSchemaInfo,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let schema_id = current_snapshot.get_next_catalog_id();
    let model = schema::ActiveModel {
        schema_id: Set(schema_id),
        begin_snapshot: Set(current_snapshot.snapshot_id),
        end_snapshot: Set(None),
        schema_name: Set(info.schema_name),
        ..Default::default()
    };

    Schema::insert(model).exec(db).await?;
    Ok(schema_id)
}
