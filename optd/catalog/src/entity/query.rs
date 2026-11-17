use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_query")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub query_id: String,
    pub created_at: DateTimeUtc,
    pub encoding_version: i32,
    pub normalization_version: i32,
    pub dialect: String,
    pub resolution_scope: String,
    pub fingerprint: String,
    pub template: String,
    pub parameter_shape: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
