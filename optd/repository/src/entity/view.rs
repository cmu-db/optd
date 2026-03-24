use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_view")]
pub struct Model {
    /// The numeric identifier of the view.
    ///
    /// `view_id` is incremented from `next_catalog_id`
    /// in the `optd_snapshot` table.
    #[sea_orm(primary_key, auto_increment = false)]
    pub view_id: i64,

    /// A UUID that gives a persistent identifier for this view.
    ///
    /// The UUID is stored here for compatibility with existing
    /// lakehouse formats.
    pub view_uuid: Uuid,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The view exists starting with this snapshot id.
    pub begin_snapshot: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The view exists up to but not including this snapshot id.
    /// If `end_snapshot` is `NULL`, the view is currently valid.
    pub end_snapshot: Option<i64>,

    /// Refers to a `schema_id` from the `optd_schema` table.
    pub schema_id: i64,

    /// The name of the view, e.g. `my_view`.
    pub view_name: String,

    /// The SQL dialect of the view definition, e.g. `duckdb`.
    pub dialect: String,

    /// The SQL string that defines the view,
    /// e.g. `SELECT * FROM my_table`.
    pub sql: String,

    /// Contains a possible rename of the view columns.
    ///
    /// Can be `NULL` if no rename is set.
    pub column_aliases: Option<String>,

    #[sea_orm(belongs_to, from = "schema_id", to = "schema_id")]
    pub schema: HasOne<super::schema::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
