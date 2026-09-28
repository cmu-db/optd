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
}

impl ActiveModelBehavior for ActiveModel {}
