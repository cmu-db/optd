use sea_orm::{ConnectionTrait, DbErr, EntityTrait, QuerySelect};

use crate::{entity::snapshot, snapshot::SnapshotInfo};

pub async fn get_snapshot_info<C>(db: &C, snapshot_id: i64) -> Result<Option<SnapshotInfo>, DbErr>
where
    C: ConnectionTrait,
{
    snapshot::Entity::find_by_id(snapshot_id)
        .select_only()
        .column(snapshot::Column::SnapshotId)
        .column(snapshot::Column::SchemaVersion)
        .column(snapshot::Column::NextCatalogId)
        .column(snapshot::Column::NextFileId)
        .into_partial_model::<SnapshotInfo>()
        .one(db)
        .await
}
