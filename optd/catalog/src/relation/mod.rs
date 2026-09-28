mod create_relation;
mod drop_relation;
mod get_all_relations;
mod get_relation;

use crate::entity::column::{self, ColumnType};

pub use create_relation::*;
pub use drop_relation::*;
pub use get_all_relations::*;
pub use get_relation::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRelationInfo {
    pub schema_name: String,
    pub relation_name: String,
    pub columns: Vec<CreateColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateColumnInfo {
    pub column_name: String,
    pub column_type: ColumnType,
    pub nulls_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRelationInfo {
    pub relation_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRelationInfo {
    pub relation_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationInfo {
    pub relation_id: i64,
    pub schema_id: i64,
    pub relation_name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub column_id: i64,
    pub column_name: String,
    pub column_type: ColumnType,
    pub nulls_allowed: bool,
}

pub(crate) fn build_columns(columns: Vec<column::Model>) -> Vec<ColumnInfo> {
    columns
        .into_iter()
        .map(|column| ColumnInfo {
            column_id: column.column_id,
            column_name: column.column_name,
            column_type: column.column_type,
            nulls_allowed: column.nulls_allowed,
        })
        .collect()
}
