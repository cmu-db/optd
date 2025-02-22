//! A scalar column reference operator.

use crate::{cascades::ir::OperatorData, operators::scalar::ScalarOperator};
use serde::Deserialize;

/// A scalar operator that references a column by index.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ColumnRef<Value> {
    /// The index of the referenced column.
    pub column_index: Value,
}

impl ColumnRef<OperatorData> {
    /// Create a new column reference.
    pub fn new(column_index: i64) -> Self {
        Self {
            column_index: OperatorData::Int64(column_index),
        }
    }
}

/// Creates a column reference scalar operator.
pub fn column_ref<Scalar>(column_index: i64) -> ScalarOperator<OperatorData, Scalar> {
    ScalarOperator::ColumnRef(ColumnRef::new(column_index))
}
