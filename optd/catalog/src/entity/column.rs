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
    /// Persistence-only row identity with no catalog semantics.
    #[sea_orm(primary_key)]
    row_id: i64,
    /// Owning relation identifier.
    pub relation_id: i64,
    /// Stable identifier scoped to the owning relation.
    pub column_id: i64,
    /// First snapshot in which this version is visible.
    pub begin_snapshot: i64,
    /// First snapshot in which this version is no longer visible.
    pub end_snapshot: Option<i64>,
    /// Position of this column version in the visible relation schema.
    pub column_order: i64,
    /// Name of this column version.
    pub column_name: String,
    /// Arrow type of this column version.
    pub column_type: ColumnType,
    /// Whether this column version accepts null values.
    pub nulls_allowed: bool,
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
