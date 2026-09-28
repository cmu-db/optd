use sea_orm::{ConnectionTrait, DbErr, EntityTrait, QuerySelect};

use crate::{
    entity::snapshot,
    snapshot::{SnapshotId, SnapshotInfo},
};

pub(crate) async fn get_snapshot_info<C>(
    db: &C,
    snapshot_id: SnapshotId,
) -> Result<Option<SnapshotInfo>, DbErr>
where
    C: ConnectionTrait,
{
    snapshot::Entity::find_by_id(snapshot_id.as_i64())
        .select_only()
        .column(snapshot::Column::SnapshotId)
        .column(snapshot::Column::NextCatalogId)
        .into_tuple::<(i64, i64)>()
        .one(db)
        .await
        .map(|row| {
            row.map(|(snapshot_id, next_catalog_id)| SnapshotInfo {
                snapshot_id: SnapshotId::from_raw(snapshot_id),
                next_catalog_id,
            })
        })
}
