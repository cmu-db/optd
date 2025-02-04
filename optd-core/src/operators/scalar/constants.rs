use serde::{Deserialize, Serialize};

use crate::values::OptdValue;

/// Constants that can appear in scalar expressions.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Constant<Value> {
    pub value: Value,
}

impl Constant<OptdValue> {
    /// Create a new constant (TODO(alexis): make enum later)
    pub fn new(value: OptdValue) -> Self {
        Self {
            value,
        }
    }
}
