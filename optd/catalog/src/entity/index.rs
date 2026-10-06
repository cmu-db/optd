//! SeaORM persistence model for temporal relational index definitions.

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_index")]
pub struct Model {
    /// Persistence-only row identity with no catalog semantics.
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Stable identifier shared by temporal versions of the index.
    pub index_id: i64,
    /// Stable identifier of the indexed relation.
    pub relation_id: i64,
    /// First snapshot in which this index version is visible.
    pub begin_snapshot: i64,
    /// First snapshot in which this index version is no longer visible.
    pub end_snapshot: Option<i64>,
    /// Backend-visible index name.
    pub index_name: String,
    /// Backend access method, such as `btree`, `hash`, `gist`, `gin`, or `brin`.
    pub access_method: String,
    /// Whether the index enforces uniqueness.
    pub is_unique: bool,
    /// Whether null values compare as equal for uniqueness enforcement.
    pub nulls_not_distinct: bool,
    /// Ordered key and included-column descriptors encoded as JSON.
    pub fields: Json,
    /// Current backend build/usability state.
    pub status: IndexStatus,
}

impl ActiveModelBehavior for ActiveModel {}

/// Physical usability state reported by the backing relational database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(16))")]
pub enum IndexStatus {
    /// The index is being built and must not be selected by the optimizer.
    #[sea_orm(string_value = "BUILDING")]
    Building,
    /// The index is complete and available for planning.
    #[sea_orm(string_value = "READY")]
    Ready,
    /// The backend reports the index as invalid or otherwise unusable.
    #[sea_orm(string_value = "INVALID")]
    Invalid,
}
