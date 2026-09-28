use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::{
    entity::{prelude::*, snapshot},
    snapshot::{SnapshotId, SnapshotInfo},
};

/// Commits the current snapshot metadata.
pub(crate) async fn commit_snapshot<C>(
    db: &C,
    current_snapshot: SnapshotInfo,
) -> Result<SnapshotId, DbErr>
where
    C: ConnectionTrait,
{
    let model = snapshot::ActiveModel {
        next_catalog_id: Set(current_snapshot.next_catalog_id),
        ..Default::default()
    };

    let result = Snapshot::insert(model).exec(db).await?;
    Ok(SnapshotId::from_raw(result.last_insert_id))
}
