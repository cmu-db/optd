use crate::{
    entity::snapshot,
    snapshot::{SnapshotId, SnapshotInfo, get_snapshot_info},
};
use sea_orm::{ConnectionTrait, DbErr, EntityTrait, QueryOrder};

pub(crate) async fn get_current_snapshot_info<C: ConnectionTrait>(
    db: &C,
) -> Result<Option<SnapshotInfo>, DbErr> {
    let row = snapshot::Entity::find()
        .order_by_desc(snapshot::Column::SnapshotId)
        .one(db)
        .await?;
    match row {
        Some(row) => get_snapshot_info(db, SnapshotId::from_raw(row.snapshot_id)).await,
        None => Ok(None),
    }
}
