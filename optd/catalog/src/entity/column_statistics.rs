//! SeaORM persistence model for column statistics payloads.

use sea_orm::entity::prelude::*;

/// Persistence row for one column in an immutable statistics payload.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_column_statistics")]
pub struct Model {
    /// Persistence-only row identity with no catalog semantics.
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Stable identifier of the statistics definition that owns this value.
    pub statistics_id: i64,
    /// `begin_snapshot` of the exact temporal statistics definition version.
    pub statistics_begin_snapshot: i64,
    /// Catalog snapshot of the relation-level payload containing this value.
    pub catalog_snapshot_id: i64,
    /// Relation-scoped durable column identifier.
    pub column_id: i64,
    /// Number of non-null values in the collected population.
    pub frequency: Option<i64>,
    /// Number of distinct non-null values.
    pub distinct_count: Option<i64>,
    /// JSON-encoded typed minimum value.
    pub lower_bound: Option<String>,
    /// JSON-encoded typed maximum value.
    pub upper_bound: Option<String>,
}

/// This persistence entity currently declares no SeaORM relationships.
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
