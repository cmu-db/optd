use serde::{Deserialize, Serialize};

use super::ScalarOperator;

/// Column reference
// TODO(yuchen): add proper catalog integration, mock for now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef<Metadata> {
    pub table_name: Metadata,
    pub column_name: Metadata,
}

/// Create a new column reference operator.
/// TODO(alexis): this should be somewhere else
pub fn unqualified_column_ref<Scalar>(column_name: &str) -> ScalarOperator<Scalar> {
    ScalarOperator::ColumnRef(ColumnRef {
        column_name: column_name.to_string(),
        table_name: None,
    })
}
pub fn qualified_column_ref<Scalar>(table_name: &str, column_name: &str) -> ScalarOperator<Scalar> {
    ScalarOperator::ColumnRef(ColumnRef {
        column_name: column_name.to_string(),
        table_name: Some(table_name.to_string()),
    })
}
