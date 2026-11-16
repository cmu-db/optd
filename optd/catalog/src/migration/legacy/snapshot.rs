use sea_orm::{
    entity::prelude::*,
    sea_query::prelude::{Utc, chrono},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_snapshot")]
pub struct Model {
    /// Monotonically increasing snapshot identifier.
    #[sea_orm(primary_key)]
    pub snapshot_id: i64,
    /// Time at which the snapshot was committed.
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub snapshot_time: chrono::DateTime<Utc>,
    /// Next identifier available for a catalog object.
    pub next_catalog_id: i64,
}

impl ActiveModelBehavior for ActiveModel {}
