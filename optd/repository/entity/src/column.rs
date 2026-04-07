use std::{
    fmt::{self, Display},
    ops::{Deref, DerefMut},
    str::FromStr,
};

use arrow_schema::{ArrowError, DataType};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_column")]
pub struct Model {
    /// Internal surrogate primary key.
    ///
    /// Not part of the DuckLake specification. Introduced for ORM convenience
    /// to uniquely identify each row/version.
    #[sea_orm(primary_key)]
    pub id: i64,

    /// Refers to a `table_id` from the `optd_table` table.
    pub table_id: i64,

    /// The numeric identifier of the column.
    ///
    /// If the Parquet file includes a field identifier, it corresponds
    /// to the file's `field_id`. This identifier should remain consistent
    /// throughout all versions of the column, until it's dropped.
    /// The `column_id` must be unique per table.
    pub column_id: u64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// This version of the column exists starting with this snapshot id.
    pub begin_snapshot: i64,

    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// This version of the column exists up to but not including this snapshot id.
    /// If `end_snapshot` is `NULL`, this version of the column is currently valid.
    pub end_snapshot: Option<i64>,

    /// Defines the position of the column in the list of columns.
    ///
    /// It needs to be unique within a snapshot but does not have to be contiguous
    /// (gaps are allowed).
    pub column_order: i64,

    /// The name of this version of the column, e.g. `my_column`.
    pub column_name: String,

    /// The type of this version of the column as defined in the list of data types.
    pub column_type: ColumnType,

    /// The initial default value as the column is being created (e.g., in `ALTER TABLE`),
    /// encoded as a string.
    ///
    /// Can be `NULL`.
    pub initial_default: Option<String>,

    /// The operational default value used during inserts and updates (e.g., in `INSERT`),
    /// encoded as a string.
    ///
    /// Can be `NULL`.
    pub default_value: Option<String>,

    /// Whether `NULL` values are allowed in this version of the column.
    ///
    /// Default values must be set if this is `false`.
    pub nulls_allowed: bool,

    /// The `column_id` of the parent column.
    ///
    /// This is `NULL` for top-level and non-nested columns.
    /// For example, for `STRUCT` types, this refers to the parent struct column.
    pub parent_column: Option<i64>,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug, PartialEq, Eq, DeriveValueType)]
#[sea_orm(value_type = "String", column_type = "Text")]
pub struct ColumnType(pub DataType);

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ColumnType {
    type Err = ArrowError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<DataType> for ColumnType {
    fn from(value: DataType) -> Self {
        Self(value)
    }
}

impl From<ColumnType> for DataType {
    fn from(value: ColumnType) -> Self {
        value.0
    }
}

impl Deref for ColumnType {
    type Target = DataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ColumnType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
