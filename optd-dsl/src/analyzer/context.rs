use super::hir::Identifier;
use crate::analyzer::hir::Value;
use std::{collections::HashMap, sync::Arc};

/// A stack-based variable binding system that implements lexical scoping.
///
/// This context maintains a stack of scopes where each scope is a hashmap
/// mapping variable names to values. Variable lookup follows lexical
/// scoping rules, searching from innermost (top of stack) to outermost
/// (bottom of stack) scope.
///
/// The current (innermost) scope is mutable, while all previous scopes
/// are immutable and stored as Arc for efficient cloning.
#[derive(Debug, Clone)]
pub struct Context {
    /// Previous scopes (outer lexical scopes), stored as immutable Arc references
    previous_scopes: Vec<Arc<HashMap<Identifier, Value>>>,

    /// Current scope (innermost) that can be directly modified
    current_scope: HashMap<Identifier, Value>,
}

impl Context {
    /// Creates a new context with the given initial bindings as the global scope.
    ///
    /// # Arguments
    ///
    /// * `initial_bindings` - Initial variable bindings to populate the global scope
    ///
    /// # Returns
    ///
    /// A new `Context` instance with one scope containing the initial bindings
    pub fn new(initial_bindings: HashMap<Identifier, Value>) -> Self {
        Self {
            previous_scopes: Vec::new(),
            current_scope: initial_bindings,
        }
    }

    /// Pushes the current scope onto the stack of previous scopes and creates
    /// a new empty current scope.
    ///
    /// This creates a new lexical scope in which variables can be bound
    /// without affecting bindings in outer scopes.
    pub fn push_scope(&mut self) {
        let old_scope = std::mem::take(&mut self.current_scope);
        self.previous_scopes.push(Arc::new(old_scope));
    }

    /// Looks up a variable in the context, starting from the innermost scope.
    ///
    /// Following lexical scoping rules, this searches for the variable
    /// from the innermost scope outward, returning the first match found.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the variable to look up
    ///
    /// # Returns
    ///
    /// Some reference to the value if found, None otherwise
    pub fn lookup(&self, name: &str) -> Option<&Value> {
        // First check the current scope
        if let Some(value) = self.current_scope.get(name) {
            return Some(value);
        }

        // Then check previous scopes from innermost to outermost
        for scope in self.previous_scopes.iter().rev() {
            if let Some(value) = scope.get(name) {
                return Some(value);
            }
        }

        None
    }

    /// Binds a variable in the current (innermost) scope.
    ///
    /// If the variable already exists in the current scope,
    /// its value will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the variable to bind
    /// * `val` - The value to bind to the variable
    pub fn bind(&mut self, name: String, val: Value) {
        self.current_scope.insert(name, val);
    }
}
