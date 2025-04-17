use super::{
    errors::AnalyzerErrorKind,
    hir::{ExprMetadata, Identifier, NoMetadata, TypedSpan},
};
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
pub struct Context<M: ExprMetadata = NoMetadata> {
    /// Previous scopes (outer lexical scopes), stored as immutable Arc references.
    previous_scopes: Vec<Arc<HashMap<Identifier, Value<M>>>>,

    /// Current scope (innermost) that can be directly modified.
    current_scope: HashMap<Identifier, Value<M>>,
}

/// A default implementation for the Context struct, since it cannot be automatically
/// inferred due to the generic type M.
impl<M: ExprMetadata> Default for Context<M> {
    fn default() -> Self {
        Context {
            previous_scopes: Vec::default(),
            current_scope: HashMap::default(),
        }
    }
}

impl<M: ExprMetadata> Context<M> {
    /// Creates a new context with the given initial bindings as the global scope.
    ///
    /// # Arguments
    ///
    /// * `initial_bindings` - Initial variable bindings to populate the global scope.
    ///
    /// # Returns
    ///
    /// A new `Context` instance with one scope containing the initial bindings.
    pub fn new(initial_bindings: HashMap<Identifier, Value<M>>) -> Self {
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
    /// * `name` - The name of the variable to look up.
    ///
    /// # Returns
    ///
    /// Some reference to the value if found, None otherwise.
    pub fn lookup(&self, name: &str) -> Option<&Value<M>> {
        // First check the current scope.
        if let Some(value) = self.current_scope.get(name) {
            return Some(value);
        }

        // Then check previous scopes from innermost to outermost.
        for scope in self.previous_scopes.iter().rev() {
            if let Some(value) = scope.get(name) {
                return Some(value);
            }
        }

        None
    }

    /// Merges bindings from another context into this context.
    ///
    /// This function takes ownership of another context and moves all bindings
    /// from its current scope into this context's current scope. It's useful
    /// when combining results from pattern matching operations.
    ///
    /// # Arguments
    ///
    /// * `other` - The context to merge from (consumed by this operation).
    pub fn merge(&mut self, other: Context<M>) {
        // Move bindings from other's current scope into our current scope.
        for (name, val) in other.current_scope {
            self.current_scope.insert(name, val);
        }
    }

    /// Binds a variable in the current (innermost) scope.
    ///
    /// If the variable already exists in the current scope,
    /// its value will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the variable to bind.
    /// * `val` - The value to bind to the variable.
    pub fn bind(&mut self, name: String, val: Value<M>) {
        self.current_scope.insert(name, val);
    }

    /// Gets all values from all scopes in the context.
    ///
    /// This retrieves values from all lexical scopes, including both the current
    /// scope and all previous (outer) scopes.
    ///
    /// # Returns
    ///
    /// A vector containing references to all values in the context.
    pub fn get_all_values(&self) -> Vec<&Value<M>> {
        self.previous_scopes
            .iter()
            .flat_map(|scope| scope.values())
            .chain(self.current_scope.values())
            .collect()
    }
}

impl Context<TypedSpan> {
    /// Attempts to bind a variable in the current scope, returning an error if
    /// the variable is already defined in the current scope.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the variable to bind
    /// * `val` - The value to bind to the variable
    ///
    /// # Returns
    ///
    /// `Ok(())` if the binding was successful, or a `AnalyzerErrorKind` if
    /// the variable was already defined in the current scope
    pub fn try_bind(
        &mut self,
        name: String,
        val: Value<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        if let Some(existing_val) = self.current_scope.get(&name) {
            // We found an existing binding, so return an error
            Err(AnalyzerErrorKind::new_duplicate_identifier(
                &name,
                &existing_val.metadata.span,
                &val.metadata.span,
            ))
        } else {
            // No existing binding, so insert the new one
            self.current_scope.insert(name, val);
            Ok(())
        }
    }
}
