use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    api::snapshot::SnapshotInfo,
    entity::{prelude::*, snapshot},
};

/// Commits the current snapshot metadata.
pub async fn commit_snapshot<C>(db: &C, current_snapshot: SnapshotInfo) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let model = snapshot::ActiveModel {
        schema_version: Set(current_snapshot.schema_version),
        next_catalog_id: Set(current_snapshot.next_catalog_id),
        next_file_id: Set(current_snapshot.next_file_id),
        ..Default::default()
    };

    Snapshot::insert(model).exec(db).await?;
    Ok(())
}
