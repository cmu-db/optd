mod create_index;
mod drop_index;
mod get_indexes;

use serde::{Deserialize, Serialize};

pub use crate::entity::index::IndexStatus;
pub(crate) use create_index::*;
pub(crate) use drop_index::*;
pub(crate) use get_indexes::*;
pub use optd_core::{NullOrdering, SortDirection};

/// Role played by one column in an index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexFieldRole {
    /// Column participates in index ordering and lookup semantics.
    Key,
    /// Column is stored only to support covering index scans.
    Include,
}

/// One ordered column descriptor in an index definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexField {
    /// Whether the column is a lookup key or an included payload column.
    pub role: IndexFieldRole,
    /// Relation-scoped durable column identifier.
    pub column_id: i64,
    /// Key direction, absent for included columns or backend-default ordering.
    pub direction: Option<SortDirection>,
    /// Key null placement, absent for included columns or backend-default ordering.
    pub null_order: Option<NullOrdering>,
    /// Backend collation name or identifier, when explicitly selected.
    pub collation: Option<String>,
    /// Backend operator-class name or identifier, when explicitly selected.
    pub operator_class: Option<String>,
}

/// Description used to publish a relational index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexInfo {
    /// Stable identifier of the indexed relation.
    pub relation_id: i64,
    /// Backend-visible index name.
    pub index_name: String,
    /// Backend access method, such as `btree` or `hash`.
    pub access_method: String,
    /// Whether the index enforces uniqueness.
    pub is_unique: bool,
    /// Whether null values compare as equal for uniqueness enforcement.
    pub nulls_not_distinct: bool,
    /// Ordered key fields followed by optional included fields.
    pub fields: Vec<IndexField>,
    /// Physical usability state reported by the backing database.
    pub status: IndexStatus,
}

/// Identifies a relational index to drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropIndexInfo {
    /// Stable index identifier.
    pub index_id: i64,
}

/// Identifies one relational index to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetIndexInfo {
    /// Stable index identifier.
    pub index_id: i64,
}

/// Snapshot-visible relational index descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    /// Stable index identifier.
    pub index_id: i64,
    /// Stable identifier of the indexed relation.
    pub relation_id: i64,
    /// Backend-visible index name.
    pub index_name: String,
    /// Backend access method.
    pub access_method: String,
    /// Whether the index enforces uniqueness.
    pub is_unique: bool,
    /// Whether null values compare as equal for uniqueness enforcement.
    pub nulls_not_distinct: bool,
    /// Ordered key fields followed by optional included fields.
    pub fields: Vec<IndexField>,
    /// Physical usability state reported by the backing database.
    pub status: IndexStatus,
}

#[cfg(test)]
mod tests {
    use super::{IndexField, IndexFieldRole, NullOrdering, SortDirection};

    #[test]
    fn index_ordering_uses_shared_core_serde() {
        let field = IndexField {
            role: IndexFieldRole::Key,
            column_id: 7,
            direction: Some(SortDirection::Desc),
            null_order: Some(NullOrdering::Last),
            collation: None,
            operator_class: None,
        };
        let encoded = serde_json::to_value(&field).unwrap();
        assert_eq!(encoded["direction"], "Desc");
        assert_eq!(encoded["null_order"], "Last");
        assert_eq!(
            serde_json::from_value::<IndexField>(encoded).unwrap(),
            field
        );
    }

    #[test]
    fn index_ordering_rejects_unknown_json_names() {
        let error = serde_json::from_value::<IndexField>(serde_json::json!({
            "role": "KEY",
            "column_id": 7,
            "direction": "Sideways",
            "null_order": null,
            "collation": null,
            "operator_class": null
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }
}
