//! A scalar column reference operator.

use crate::{operators::scalar::ScalarOperator, values::OptdValue};
use serde::Deserialize;

/// A scalar operator that references a column by index.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ColumnRef<Value> {
    /// The index of the referenced column.
    pub column_index: Value,
}

impl ColumnRef<OptdValue> {
    /// Create a new column reference.
    pub fn new(column_index: i64) -> Self {
        Self {
            column_index: OptdValue::Int64(column_index),
        }
    }
}

/// Creates a column reference scalar operator.
pub fn column_ref<Scalar>(column_index: i64) -> ScalarOperator<OptdValue, Scalar> {
    ScalarOperator::ColumnRef(ColumnRef::new(column_index))
}
