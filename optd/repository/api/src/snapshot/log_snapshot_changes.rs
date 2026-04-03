use itertools::Itertools;
use sea_orm::{ConnectionTrait, DbErr, EntityTrait};

use crate::snapshot::SnapshotInfo;

use crate::entity::{
    prelude::*,
    snapshot_changes::{self, ChangesMade},
};

pub async fn log_snapshot_changes<C>(
    db: &C,
    new_snapshot_id: i64,
    changes_made: &[ChangesMade],
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let changes_made = changes_made.iter().map(ChangesMade::to_string).join(",");

    let model = snapshot_changes::ActiveModel {
        snapshot_id: sea_orm::Set(new_snapshot_id),
        changes_made: sea_orm::Set(changes_made),
        ..Default::default()
    };

    SnapshotChanges::insert(model).exec(db).await?;
    Ok(())
}
