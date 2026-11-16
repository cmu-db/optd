//! Advanced statistics are additive temporal objects, independent of basic statistics.
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_adv_stats")]
pub struct Model {
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Stable ID from the shared catalog allocator; unchanged by ordinary updates.
    pub statistics_id: i64,
    pub relation_id: i64,
    pub statistics_name: String,
    pub statistics_kind: String,
    pub column_ids: Json,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub value_encoding_version: i32,
    /// JSON text, rather than a JSON value, preserves large Decimal128 bounds exactly.
    pub contents: String,
    pub collected_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
