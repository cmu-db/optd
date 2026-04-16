use sea_orm::entity::prelude::*;

/// The query table stores information about each query, including the SQL text.
/// This table is not in the DuckLake schema.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_query")]
pub struct Model {
    /// The numeric identifier of the query.
    #[sea_orm(primary_key)]
    pub query_id: i64,

    /// The SQL text for this query.
    pub sql: String,

    #[sea_orm(has_many)]
    pub query_instances: HasMany<super::query_instance::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
