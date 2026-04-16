use sea_orm::entity::prelude::*;

/// The query plan table stores JSON-encoded plans associated with query instances.
/// This table is not in the DuckLake schema.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_query_plan")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,

    /// Refers to an `id` from the `optd_query_instance` table.
    pub query_instance_id: i64,

    /// The query plan, encoded as JSON.
    pub plan: Json,

    /// Description of this plan, such as `initial_plan` or `final_plan`.
    pub description: String,

    #[sea_orm(belongs_to, from = "query_instance_id", to = "id")]
    pub query_instance: HasOne<super::query_instance::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
