use sea_orm::{ConnectionTrait, DbErr, EntityTrait, QueryOrder, QuerySelect};

use crate::{api::snapshot::SnapshotInfo, entity::snapshot};

pub async fn get_current_snapshot_info<C>(db: &C) -> Result<Option<SnapshotInfo>, DbErr>
where
    C: ConnectionTrait,
{
    snapshot::Entity::find()
        .select_only()
        .column(snapshot::Column::SnapshotId)
        .column(snapshot::Column::SchemaVersion)
        .column(snapshot::Column::NextCatalogId)
        .column(snapshot::Column::NextFileId)
        .order_by_desc(snapshot::Column::SnapshotId)
        .into_partial_model::<SnapshotInfo>()
        .one(db)
        .await
}
