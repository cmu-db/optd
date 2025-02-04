use serde::{Deserialize, Serialize};

use crate::values::OptdValue;

/// Column reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef<Value> {
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
