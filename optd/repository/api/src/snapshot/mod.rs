mod commit_snapshot_info;
mod get_current_snapshot_info;
mod get_snapshot_info;
mod get_snapshot_info_at;
mod log_snapshot_changes;

use crate::entity::snapshot;
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DbErr, DerivePartialModel};

pub use commit_snapshot_info::*;
pub use get_current_snapshot_info::*;
pub use get_snapshot_info::*;
pub use get_snapshot_info_at::*;
pub use log_snapshot_changes::*;

#[derive(Debug, Clone, PartialEq, Eq, DerivePartialModel)]
#[sea_orm(entity = "snapshot::Entity")]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub schema_version: i64,
    pub next_catalog_id: i64,
    pub next_file_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotSelector {
    Current,
    Id(i64),
    At(DateTime<Utc>),
    Info(SnapshotInfo),
}

pub async fn resolve_snapshot_selector<C>(
    db: &C,
    selector: SnapshotSelector,
) -> Result<SnapshotInfo, DbErr>
where
    C: ConnectionTrait,
{
    match selector {
        SnapshotSelector::Current => Ok(get_current_snapshot_info(db).await?.unwrap_or_default()),
        SnapshotSelector::Id(snapshot_id) => get_snapshot_info(db, snapshot_id)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("snapshot {snapshot_id} not found"))),
        SnapshotSelector::At(time) => get_snapshot_info_at(db, time)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("snapshot at {time} not found"))),
        SnapshotSelector::Info(snapshot) => Ok(snapshot),
    }
}

impl Default for SnapshotInfo {
    fn default() -> Self {
        Self {
            snapshot_id: 0,
            schema_version: 0,
            next_catalog_id: 1,
            next_file_id: 0,
        }
    }
}

impl SnapshotInfo {
    pub fn get_next_catalog_id(&mut self) -> i64 {
        let id = self.next_catalog_id;
        self.next_catalog_id += 1;
        id
    }
}
