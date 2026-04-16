use sea_orm::{
    entity::prelude::*,
    sea_query::prelude::{Utc, chrono},
};

/// The query instance table stores information about each query instance,
/// including the query itself and the snapshot it is running against.
/// This table is not in the DuckLake schema.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_query_instance")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,

    /// Refers to a `query_id` from the `optd_query` table.
    pub query_id: i64,

    /// The snapshot id that this query instance is running against.
    pub snapshot_id: i64,

    /// The timestamp at which this query instance was created.
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub query_time: chrono::DateTime<Utc>,

    #[sea_orm(belongs_to, from = "query_id", to = "query_id")]
    pub query: HasOne<super::query::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
