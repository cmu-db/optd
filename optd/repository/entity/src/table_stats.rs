use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_table_stats")]
pub struct Model {
    /// Internal surrogate primary key.
    ///
    /// Not part of the DuckLake specification. Introduced for ORM convenience
    /// to uniquely identify each row.
    #[sea_orm(primary_key)]
    id: i64,

    /// Refers to a `table_id` from the `optd_table` table.
    pub table_id: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The table exists starting with this snapshot id.
    pub begin_snapshot: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The table exists up to but not including this snapshot id.
    /// If `end_snapshot` is `NULL`, the table is currently valid.
    pub end_snapshot: Option<i64>,

    /// The total number of rows in the table.
    ///
    /// This value can be approximate.
    pub record_count: i64,

    /// The next row identifier for newly inserted rows.
    ///
    /// Used for row lineage tracking.
    pub next_row_id: i64,

    /// The total file size (in bytes) of all data files in the table.
    ///
    /// This value can be approximate.
    pub file_size_bytes: i64,
}

impl ActiveModelBehavior for ActiveModel {}
