use sea_orm::{
    entity::prelude::*,
    sea_query::prelude::{Utc, chrono},
};

/// The query instance table stores information about each query instance,
/// including the query itself, the snapshot it is running against, and the initial/final plans.
/// This table is not in the DuckLake schema.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_query_instance")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i64,

    /// Refers to a `query_id` from the `optd_query` table.
    pub query_id: i64,

    /// The snapshot id that this query instance is running against.
    pub snapshot_id: i64,

    /// The timestamp at which this query instance was created.
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub query_time: chrono::DateTime<Utc>,

    /// The initial plan for the query instance, encoded as JSON.
    pub initial_plan: Option<Json>,

    /// The final plan for the query instance, encoded as JSON.
    pub final_plan: Option<Json>,

    #[sea_orm(belongs_to, from = "query_id", to = "query_id")]
    pub query: HasOne<super::query::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
