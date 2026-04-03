use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    entity::{prelude::*, snapshot},
    snapshot::SnapshotInfo,
};

/// Commits the current snapshot metadata.
pub async fn commit_snapshot<C>(db: &C, current_snapshot: SnapshotInfo) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let model = snapshot::ActiveModel {
        schema_version: Set(current_snapshot.schema_version),
        next_catalog_id: Set(current_snapshot.next_catalog_id),
        next_file_id: Set(current_snapshot.next_file_id),
        ..Default::default()
    };

    let result = Snapshot::insert(model).exec(db).await?;
    Ok(result.last_insert_id)
}
