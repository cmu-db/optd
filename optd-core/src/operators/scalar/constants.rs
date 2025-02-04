use serde::{Deserialize, Serialize};

use crate::values::OptdValue;

/// Constants that can appear in scalar expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constant<Value> {
    value: Value,
}

impl Constant<OptdValue> {
    /// Create a new constant (TODO(alexis): make enum later)
    pub fn new(value: i64) -> Self {
        Self {
            value: OptdValue::Int64(value),
        }
    }
}
