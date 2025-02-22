use crate::analyzer::hir::Expr;
use std::collections::HashMap;

/// A stack-based variable binding system that implements lexical scoping.
///
/// This context maintains a stack of scopes where each scope is a hashmap
/// mapping variable names to expressions. Variable lookup follows lexical
/// scoping rules, searching from innermost (top of stack) to outermost
/// (bottom of stack) scope.
///
/// The context always contains at least one scope (the global scope).
pub struct Context {
    /// Stack of scopes, where each scope is a map of variable names to expressions.
    /// The last element is the current (innermost) scope.
    scopes: Vec<HashMap<String, Expr>>,
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
    pub fn new(initial_bindings: HashMap<String, Expr>) -> Self {
        let mut context = Self { scopes: Vec::new() };
        // Start with a scope containing the initial bindings
        context.scopes.push(initial_bindings);
        context
    }

    /// Pushes a new empty scope onto the stack.
    ///
    /// This creates a new lexical scope in which variables can be bound
    /// without affecting bindings in outer scopes.
    pub fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    /// Pops the topmost scope from the stack.
    ///
    /// # Panics
    ///
    /// Panics if attempting to pop the last remaining scope (global scope)
    pub fn pop_scope(&mut self) {
        if self.scopes.len() <= 1 {
            panic!("Cannot pop global scope");
        }
        self.scopes.pop();
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
    /// Some reference to the expression if found, None otherwise
    pub fn lookup(&self, name: &str) -> Option<&Expr> {
        for scope in self.scopes.iter().rev() {
            if let Some(expr) = scope.get(name) {
                return Some(expr);
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
    /// * `expr` - The expression to bind to the variable
    pub fn bind(&mut self, name: String, expr: Expr) {
        // We're always guaranteed to have at least one scope
        self.scopes.last_mut().unwrap().insert(name, expr);
    }
}
