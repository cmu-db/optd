//! One global planning-metadata history; not source-data time travel.
mod commit;
mod get_current_snapshot_info;
mod get_snapshot_info;

pub(crate) use commit::*;
pub(crate) use get_current_snapshot_info::*;
pub(crate) use get_snapshot_info::*;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// Identifier of an immutable planning-metadata snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotId(i64);
impl SnapshotId {
    pub const INITIAL: Self = Self(0);
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}
impl Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Immutable snapshot descriptor. Schema version is distinct from the database format version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub snapshot_id: SnapshotId,
    pub schema_version: i64,
    pub next_catalog_id: i64,
}
impl SnapshotInfo {
    pub(crate) fn get_next_catalog_id(&mut self) -> crate::CatalogResult<i64> {
        let id = self.next_catalog_id;
        self.next_catalog_id = id
            .checked_add(1)
            .ok_or(crate::CatalogError::CounterExhausted {
                counter: "object ID",
            })?;
        Ok(id)
    }
}

/// Typed operation summary, not a redo log or external-data change feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotChange {
    LegacyImport,
    RegisterCatalog {
        catalog_id: i64,
    },
    CreateSchema {
        schema_id: i64,
    },
    DropSchema {
        schema_id: i64,
    },
    CreateRelation {
        relation_id: i64,
    },
    DropRelation {
        relation_id: i64,
    },
    CreateIndex {
        index_id: i64,
    },
    DropIndex {
        index_id: i64,
    },
    RefreshStatistics {
        relation_id: i64,
    },
    InvalidateStatistics {
        relation_id: i64,
    },
    /// Batch form; the original single-relation summary remains decodable.
    InvalidateStatisticsBatch {
        relation_ids: Vec<i64>,
        cause: crate::statistics::StatisticsInvalidationCause,
    },
    CreateAdvancedStatistics {
        statistics_id: i64,
        relation_id: i64,
    },
    UpdateAdvancedStatistics {
        statistics_id: i64,
        relation_id: i64,
    },
    DropAdvancedStatistics {
        statistics_id: i64,
        relation_id: i64,
    },
}
impl SnapshotChange {
    pub(crate) fn structural(&self) -> bool {
        !matches!(
            self,
            Self::LegacyImport
                | Self::RefreshStatistics { .. }
                | Self::InvalidateStatistics { .. }
                | Self::InvalidateStatisticsBatch { .. }
                | Self::CreateAdvancedStatistics { .. }
                | Self::UpdateAdvancedStatistics { .. }
                | Self::DropAdvancedStatistics { .. }
        )
    }
}
