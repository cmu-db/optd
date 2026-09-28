mod create_schema;
mod drop_schema;
mod get_all_schemas;
mod get_schema;

use sea_orm::DerivePartialModel;

use crate::entity::schema;

pub use create_schema::*;
pub use drop_schema::*;
pub(crate) use get_all_schemas::*;
pub(crate) use get_schema::*;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaInfo {
    pub schema_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropSchemaInfo {
    pub schema_id: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GetSchemaInfo {
    pub schema_id: i64,
}

#[derive(Debug, Clone, PartialEq, DerivePartialModel)]
#[sea_orm(entity = "schema::Entity")]
pub struct SchemaInfo {
    pub schema_id: i64,
    pub schema_name: String,
}
