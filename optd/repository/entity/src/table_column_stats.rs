use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_table_column_stats")]
pub struct Model {
    /// Internal surrogate primary key.
    ///
    /// Not part of the DuckLake specification. Introduced for ORM convenience
    /// to uniquely identify each row.
    #[sea_orm(primary_key)]
    pub id: i64,

    /// Refers to a `table_id` from the `ducklake_table` table.
    pub table_id: i64,

    /// Refers to a `column_id` from the `ducklake_column` table.
    pub column_id: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The table exists starting with this snapshot id.
    pub begin_snapshot: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// The table exists up to but not including this snapshot id.
    /// If `end_snapshot` is `NULL`, the table is currently valid.
    pub end_snapshot: Option<i64>,

    /// Flag indicating whether the column contains any `NULL` values.
    pub contains_null: bool,

    /// Flag indicating whether the column contains any `NaN` values.
    ///
    /// Only relevant for floating-point types.
    pub contains_nan: bool,

    /// The minimum value for the column, encoded as a string.
    ///
    /// This does not have to be exact but must be a lower bound.
    /// The value must be cast to the actual type for accurate comparison.
    pub min_value: Option<String>,

    /// The maximum value for the column, encoded as a string.
    ///
    /// This does not have to be exact but must be an upper bound.
    /// The value must be cast to the actual type for accurate comparison.
    pub max_value: Option<String>,

    /// Additional statistics encoded as a string.
    ///
    /// Can be `NULL`.
    pub extra_stats: Option<Json>,
}

impl ActiveModelBehavior for ActiveModel {}
