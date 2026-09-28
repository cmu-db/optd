mod commit_snapshot_info;
mod get_current_snapshot_info;
mod get_snapshot_info;
mod publish_snapshot;

use std::fmt::{self, Display};

pub(crate) use commit_snapshot_info::*;
pub(crate) use get_current_snapshot_info::*;
pub(crate) use get_snapshot_info::*;
pub(crate) use publish_snapshot::*;

/// Identifier of an immutable catalog snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotId(i64);

impl SnapshotId {
    /// Initial snapshot created with the catalog.
    pub const INITIAL: Self = Self(0);

    /// Constructs an identifier from its database representation.
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the database representation.
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}

impl Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotInfo {
    pub snapshot_id: SnapshotId,
    pub next_catalog_id: i64,
}

impl SnapshotInfo {
    pub fn get_next_catalog_id(&mut self) -> i64 {
        let id = self.next_catalog_id;
        self.next_catalog_id += 1;
        id
    }
}
