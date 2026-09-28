mod create_relation;
mod drop_relation;
mod get_all_relations;
mod get_relation;

use crate::entity::column::{self, ColumnType};

pub use crate::entity::relation::{RelationKind, StorageKind};
pub(crate) use create_relation::*;
pub use drop_relation::*;
pub(crate) use get_all_relations::*;
pub(crate) use get_relation::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseTableStorage {
    Relational,
    Files,
}

impl From<BaseTableStorage> for StorageKind {
    fn from(value: BaseTableStorage) -> Self {
        match value {
            BaseTableStorage::Relational => Self::Relational,
            BaseTableStorage::Files => Self::Files,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBaseTableInfo {
    pub schema_name: String,
    pub relation_name: String,
    pub storage: BaseTableStorage,
    pub columns: Vec<CreateColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateRelationInfo {
    pub schema_name: String,
    pub relation_name: String,
    pub relation_kind: RelationKind,
    pub storage_kind: StorageKind,
    pub columns: Vec<CreateColumnInfo>,
}

impl From<CreateBaseTableInfo> for CreateRelationInfo {
    fn from(info: CreateBaseTableInfo) -> Self {
        Self {
            schema_name: info.schema_name,
            relation_name: info.relation_name,
            relation_kind: RelationKind::Table,
            storage_kind: info.storage.into(),
            columns: info.columns,
        }
    }
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
    pub relation_kind: RelationKind,
    pub storage_kind: StorageKind,
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
