use sea_orm::{ConnectionTrait, DbErr, EntityTrait};

use crate::{
    entity::prelude::Branch,
    snapshot::{SnapshotId, SnapshotInfo, get_snapshot_info},
};

pub(crate) async fn get_current_snapshot_info<C>(
    db: &C,
    branch_name: &str,
) -> Result<Option<SnapshotInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let Some(branch) = Branch::find_by_id(branch_name).one(db).await? else {
        return Ok(None);
    };

    get_snapshot_info(db, SnapshotId::from_raw(branch.current_snapshot_id)).await
}
