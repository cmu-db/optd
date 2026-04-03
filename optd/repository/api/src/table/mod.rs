mod create_table;
mod drop_table;
mod get_all_tables;
mod get_table;

use std::sync::Arc;

use itertools::Itertools;
use optd_core::ir::table_ref::TableRef;
use sea_orm::prelude::Uuid;

use crate::entity::column::ColumnType;

pub use create_table::*;
pub use drop_table::*;
pub use get_all_tables::*;
pub use get_table::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableInfo {
    pub table_name: TableRef,
    pub columns: Vec<CreateColumnInfo>,
    // TODO(yuchen): Constraints (primary key, unique, etc.)
    // pub constraints: Constraints,
}

impl CreateTableInfo {
    pub fn arrow_schema(&self) -> arrow_schema::Schema {
        arrow_schema::Schema::new(
            self.columns
                .iter()
                .map(CreateColumnInfo::field)
                .map(Arc::new)
                .collect_vec(),
        )
    }
}

// TODO(yuchen): currently we do not handle nested columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateColumnInfo {
    pub column_name: String,
    pub column_type: ColumnType,
    pub nulls_allowed: bool,
    // TODO(yuchen): Change to use `ScalarValue` after we have `ScalarValue::try_from_string` that converts a string into a target data type.
    pub initial_default: Option<String>,
    // TODO(yuchen): Change to use `ScalarValue` after we have `ScalarValue::try_from_string` that converts a string into a target data type.
    pub default_value: Option<String>,
}

impl CreateColumnInfo {
    pub fn field(&self) -> arrow_schema::Field {
        arrow_schema::Field::new(
            self.column_name.clone(),
            self.column_type.0.clone(),
            self.nulls_allowed,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableInfo {
    pub table_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTableInfo {
    pub table_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub table_id: i64,
    pub schema_id: i64,
    pub table_uuid: Uuid,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}

impl TableInfo {
    pub fn arrow_schema(&self) -> arrow_schema::Schema {
        arrow_schema::Schema::new(
            self.columns
                .iter()
                .map(ColumnInfo::field)
                .map(Arc::new)
                .collect_vec(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub column_id: i64,
    pub column_name: String,
    pub column_type: ColumnType,
    pub nulls_allowed: bool,
    // TODO(yuchen): Change to use `ScalarValue` after we have `ScalarValue::try_from_string` that converts a string into a target data type.
    pub initial_default: Option<String>,
    // TODO(yuchen): Change to use `ScalarValue` after we have `ScalarValue::try_from_string` that converts a string into a target data type.
    pub default_value: Option<String>,
    // TODO(yuchen): currently not handled.
    pub children: Vec<ColumnInfo>,
}

impl ColumnInfo {
    pub fn field(&self) -> arrow_schema::Field {
        arrow_schema::Field::new(
            self.column_name.clone(),
            self.column_type.0.clone(),
            self.nulls_allowed,
        )
    }
}
