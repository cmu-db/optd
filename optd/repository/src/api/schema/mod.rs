mod create_schema;
mod get_all_schema_infos;

use sea_orm::{DerivePartialModel, prelude::Uuid};

use crate::entity::schema;

pub use create_schema::*;
pub use get_all_schema_infos::*;

#[derive(Debug, Clone, PartialEq, Eq, DerivePartialModel)]
#[sea_orm(entity = "schema::Entity")]
pub struct SchemaInfo {
    pub schema_id: i64,
    pub schema_uuid: Uuid,
    pub schema_name: String,
}
