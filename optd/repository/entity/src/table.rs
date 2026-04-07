use sea_orm::{ActiveValue::Set, entity::prelude::*};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_table")]
pub struct Model {
    /// Internal surrogate primary key.
    ///
    /// Not part of the DuckLake specification. Introduced for ORM convenience
    /// to uniquely identify each row/version.
    #[sea_orm(primary_key)]
    id: i64,

    /// The numeric identifier of the table.
    ///
    /// `table_id` is incremented from `next_catalog_id`
    /// in the `optd_snapshot` table.
    pub table_id: i64,

    /// A UUID that gives a persistent identifier for this table.
    ///
    /// The UUID is stored here for compatibility with existing
    /// lakehouse formats.
    pub table_uuid: Uuid,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The table exists starting with this snapshot id.
    pub begin_snapshot: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The table exists up to but not including this snapshot id.
    /// If `end_snapshot` is `NULL`, the table is currently valid.
    pub end_snapshot: Option<i64>,

    /// Refers to a `schema_id` from the `optd_schema` table.
    pub schema_id: i64,

    /// The name of the table, e.g. `my_table`.
    pub table_name: String,

    /// The SQL definition of the table, e.g. `CREATE TABLE my_table (...)`.
    /// Note: This is not part of the DuckLake specification.
    pub definition: Option<String>,
    // TODO(yuchen): handle paths.
    // /// The `data_path` of the table.
    // pub path: String,
    // /// Whether the `path` is relative to the `path` of the schema (`true`)
    // /// or an absolute path (`false`).
    // pub path_is_relative: bool,
}

impl ActiveModelBehavior for ActiveModel {
    fn new() -> Self {
        Self {
            table_uuid: Set(Uuid::new_v4()),
            ..ActiveModelTrait::default()
        }
    }
}
