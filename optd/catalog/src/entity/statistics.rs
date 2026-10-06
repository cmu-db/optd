//! SeaORM persistence model for statistics definitions.

use sea_orm::entity::prelude::*;

/// Persistence row for a temporal statistics definition.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_statistics")]
pub struct Model {
    /// Persistence-only row identity with no catalog semantics.
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Stable identifier shared by every temporal version of this definition.
    pub statistics_id: i64,
    /// Stable identifier of the relation that owns the definition.
    pub relation_id: i64,
    /// Optional user-visible name; `None` denotes the implicit table statistics definition.
    pub statistics_name: Option<String>,
    /// Statistics definition category, currently `table`.
    pub statistics_kind: String,
    /// Canonically sorted durable column IDs encoded as a JSON array.
    ///
    /// The implicit table-statistics definition uses an empty array. Future named
    /// multicolumn definitions use this vector to align payload dimensions.
    pub column_ids: Json,
    /// First catalog snapshot in which this definition version is visible.
    pub begin_snapshot: i64,
    /// First catalog snapshot in which this definition version is no longer visible.
    pub end_snapshot: Option<i64>,
}

/// This persistence entity currently declares no SeaORM relationships.
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
