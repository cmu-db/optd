use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_relation")]
pub struct Model {
    /// Persistence-only row identity with no catalog semantics.
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Stable identifier shared by temporal versions of the relation.
    pub relation_id: i64,
    /// First snapshot in which this version is visible.
    pub begin_snapshot: i64,
    /// First snapshot in which this version is no longer visible.
    pub end_snapshot: Option<i64>,
    /// Schema containing this relation version.
    pub schema_id: i64,
    /// Name of this relation version.
    pub relation_name: String,
    /// Logical behavior of this relation version.
    pub relation_kind: RelationKind,
    /// Physical storage used by this relation version.
    pub storage_kind: StorageKind,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(32))")]
pub enum RelationKind {
    #[sea_orm(string_value = "TABLE")]
    Table,
    #[sea_orm(string_value = "VIEW")]
    View,
    #[sea_orm(string_value = "MATERIALIZED_VIEW")]
    MaterializedView,
    #[sea_orm(string_value = "SEQUENCE")]
    Sequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(16))")]
pub enum StorageKind {
    #[sea_orm(string_value = "RELATIONAL")]
    Relational,
    #[sea_orm(string_value = "FILES")]
    Files,
    #[sea_orm(string_value = "NONE")]
    None,
}
