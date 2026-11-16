use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_branch")]
pub struct Model {
    /// Stable branch name.
    #[sea_orm(primary_key, auto_increment = false)]
    pub branch_name: String,
    /// Snapshot currently published as this branch's head.
    pub current_snapshot_id: i64,
}

impl ActiveModelBehavior for ActiveModel {}
