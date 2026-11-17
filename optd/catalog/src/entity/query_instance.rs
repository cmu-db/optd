use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_query_instance")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub query_instance_id: String,
    pub query_id: Option<String>,
    pub started_at: DateTimeUtc,
    pub finished_at: Option<DateTimeUtc>,
    pub status: String,
    pub encoding_version: i32,
    pub catalog_snapshot_id: Option<i64>,
    pub request: String,
    pub resolution_context: String,
    pub optimizer_configuration: String,
    pub implementation_descriptor: String,
    pub optimization_duration_ns: Option<i64>,
    pub outcome_details: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
