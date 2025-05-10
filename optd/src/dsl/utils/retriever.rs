use std::collections::HashMap;

use crate::dsl::analyzer::hir::{CoreData, GroupId, Value};
use async_trait::async_trait;

/// Defines an interface for retrieving properties associated with expression groups.
///
/// The Retriever trait provides a method for the rule engine of the optimizer to
/// access information stored in the memo table directly.
#[async_trait]
pub trait Retriever: Sync + Send {
    /// Asynchronously retrieves properties for a given expression group.
    ///
    /// # Parameters
    /// * `group_id` - The identifier of the expression group whose properties are being retrieved.
    ///
    /// # Returns
    /// A `Value` containing the logical properties associated with the specified group.
    async fn get_properties(&self, group_id: GroupId) -> Value;
}

/// A mock implementation of the Retriever trait for testing purposes.
#[derive(Clone, Default)]
pub struct MockRetriever {
    /// Maps group IDs to their associated properties.
    properties: HashMap<GroupId, Value>,
}

impl MockRetriever {
    /// Creates a new empty MockRetriever.
    pub fn new() -> Self {
        Self {
            properties: HashMap::new(),
        }
    }

    /// Registers a property value to be returned when a specific group is requested.
    pub fn register(&mut self, group_id: GroupId, value: Value) {
        self.properties.insert(group_id, value);
    }
}

#[async_trait]
impl Retriever for MockRetriever {
    async fn get_properties(&self, group_id: GroupId) -> Value {
        self.properties.get(&group_id).cloned().unwrap_or_else(|| {
            // Return a default value (None) for unknown groups.
            Value::new(CoreData::None)
        })
    }
}
