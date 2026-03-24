use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_snapshot_changes")]
pub struct Model {
    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// This is the snapshot for which the changes were recorded.
    /// Each snapshot has a corresponding entry describing its changes.
    #[sea_orm(primary_key, auto_increment = false)]
    pub snapshot_id: i64,

    /// A comma-separated list of high-level changes made by the snapshot.
    ///
    /// Each entry follows the format `<change_type>:<identifier>`, e.g.:
    /// - `created_schema:schema_name`
    /// - `created_table:table_name`
    /// - `created_view:view_name`
    /// - `inserted_into_table:table_id`
    /// - `deleted_from_table:table_id`
    /// - `compacted_table:table_id`
    /// - `dropped_schema:schema_id`
    /// - `dropped_table:table_id`
    /// - `dropped_view:view_id`
    /// - `altered_table:table_id`
    /// - `altered_view:view_id`
    ///
    /// Names are written using SQL-style quoted escaping when necessary.
    pub changes_made: String,

    /// Author of the snapshot.
    ///
    /// Can be `NULL` if no author information is provided.
    pub author: Option<String>,

    /// Commit message associated with the snapshot.
    ///
    /// Can be `NULL`.
    pub commit_message: Option<String>,

    /// Additional metadata about the commit.
    ///
    /// Can be `NULL`.
    pub commit_extra_info: Option<String>,

    #[sea_orm(belongs_to, from = "snapshot_id", to = "snapshot_id")]
    pub snapshot: HasOne<super::snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
