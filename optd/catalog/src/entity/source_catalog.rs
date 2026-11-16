//! Registrations of authoritative external catalogs; never contains credentials.
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_catalog")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub row_id: i64,
    pub catalog_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub catalog_name: String,
    pub catalog_kind: String,
    pub external_catalog_name: Option<String>,
    pub adapter_name: String,
    pub connection_profile_reference: String,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
