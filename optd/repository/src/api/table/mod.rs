mod create_table;
mod get_all_table_infos;

use sea_orm::prelude::Uuid;

pub use create_table::*;
pub use get_all_table_infos::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub id: i64,
    pub schema_id: i64,
    pub table_uuid: Uuid,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub id: i64,
    pub column_name: String,
    pub column_type: String,
    pub initial_default: Option<String>,
    pub default_value: Option<String>,
    pub nulls_allowed: bool,
    pub children: Vec<ColumnInfo>,
}
