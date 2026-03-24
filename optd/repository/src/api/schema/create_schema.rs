use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    api::snapshot::SnapshotInfo,
    entity::{prelude::*, schema},
};

pub async fn create_new_schema<C>(
    schema_name: &str,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let model = schema::ActiveModel {
        schema_id: Set(current_snapshot.get_next_catalog_id()),
        begin_snapshot: Set(current_snapshot.snapshot_id),
        end_snapshot: Set(None),
        schema_name: Set(schema_name.to_owned()),
        ..Default::default()
    };

    Schema::insert(model).exec(db).await?;
    Ok(())
}
