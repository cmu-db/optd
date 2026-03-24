use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_schema")]
pub struct Model {
    /// The numeric identifier of the schema.
    ///
    /// `schema_id` is incremented from `next_catalog_id`
    /// in the `optd_snapshot` table.
    #[sea_orm(primary_key, auto_increment = false)]
    pub schema_id: i64,
    /// A UUID that gives a persistent identifier for this schema.
    ///
    /// The UUID is stored here for compatibility with existing
    /// lakehouse formats.
    pub schema_uuid: Uuid,
    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The schema exists starting with this snapshot id.
    pub begin_snapshot: i64,
    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The schema exists up to but not including this snapshot id.
    /// If `end_snapshot` is `NULL`, the schema is currently valid.
    pub end_snapshot: Option<i64>,
    /// The name of the schema, e.g. `my_schema`.
    pub schema_name: String,
    /// The `data_path` of the schema.
    pub path: String,
    /// Whether the `path` is relative to the `data_path` of the catalog
    /// (`true`) or an absolute path (`false`).
    pub path_is_relative: bool,

    #[sea_orm(has_many)]
    pub tables: HasMany<super::table::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
