//! SeaORM persistence model for temporal statistics payloads.

use sea_orm::entity::prelude::*;

/// Persistence row for one temporal relation-level statistics payload.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_statistics_payload")]
pub struct Model {
    /// Persistence-only row identity with no catalog semantics.
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Stable identifier of the statistics definition that owns this payload.
    pub statistics_id: i64,
    /// `begin_snapshot` of the exact temporal statistics definition version.
    pub statistics_begin_snapshot: i64,
    /// First catalog snapshot in which this payload is visible.
    pub begin_snapshot: i64,
    /// First catalog snapshot in which this payload is no longer visible.
    pub end_snapshot: Option<i64>,
    /// Encoding version used by JSON-encoded typed column bounds.
    pub value_encoding_version: i32,
    /// UTC time at which collection completed.
    pub collected_at: DateTimeUtc,
    /// Number of rows in the collected population.
    pub row_count: Option<i64>,
    /// Physical size of the collected population in bytes.
    pub size_bytes: Option<i64>,
}

/// This persistence entity currently declares no SeaORM relationships.
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
