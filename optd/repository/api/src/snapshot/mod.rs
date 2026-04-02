mod commit_snapshot_info;
mod get_current_snapshot_info;
mod get_snapshot_info;
mod get_snapshot_info_at;

use crate::entity::snapshot;
use sea_orm::DerivePartialModel;

pub use commit_snapshot_info::*;
pub use get_current_snapshot_info::*;
pub use get_snapshot_info::*;
pub use get_snapshot_info_at::*;

#[derive(Debug, Clone, PartialEq, Eq, DerivePartialModel)]
#[sea_orm(entity = "snapshot::Entity")]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub schema_version: i64,
    pub next_catalog_id: i64,
    pub next_file_id: i64,
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
