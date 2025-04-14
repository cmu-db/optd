use crate::analyzer::{
    errors::AnalyzerErrorKind,
    hir::{Identifier, TypedSpan},
    types::{Type, TypeRegistry},
};
use std::collections::HashMap;

/// Represents a constraint on a type.
///
/// Type constraints are used by the solver to determine the concrete types
/// of expressions in the type checking phase. Each constraint includes the
/// source code location where it was generated to provide useful error messages.
#[derive(Debug)]
enum Constraint {
    /// Subtyping relationship: `target_type >: sub_types`
    ///
    /// This constraint enforces that `target_type` is a supertype of all `sub_types`,
    /// allowing substitution of more specific types where a more general type is expected.
    Subtypes {
        target_type: TypedSpan,
        sub_types: Vec<TypedSpan>,
    },

    /// Equality constraint: `target_type = other_type`
    ///
    /// This constraint enforces that `target_type` is equal to `other_type`.
    Equal {
        target_type: TypedSpan,
        other_type: TypedSpan,
    },

    /// Field access: `inner.field = outer`
    ///
    /// This constraint enforces that the type `inner` has a field named `field`
    /// with a type that matches `outer`. Used for struct field access.
    FieldAccess {
        inner: TypedSpan,
        field: Identifier,
        outer: TypedSpan,
    },
}

/// The constraint solver that enforces type rules and resolves unknown types.
///
/// The solver collects constraints during type checking and then resolves them
/// to determine concrete types for all expressions in the program.
#[derive(Debug)]
pub struct Solver<'a> {
    /// The type registry that holds all known types
    pub(super) registry: &'a TypeRegistry,
    /// The set of collected type constraints to be solved
    constraints: Vec<Constraint>,
    /// Maps unknown type IDs to their resolved concrete types
    resolved_unknown: HashMap<usize, Type>,
}

impl<'a> Solver<'a> {
    /// Creates a new instance of the constraint solver, initializing it with
    /// a type registry.
    pub(super) fn new(registry: &'a TypeRegistry) -> Self {
        Self {
            registry,
            constraints: Vec::new(),
            resolved_unknown: HashMap::new(),
        }
    }

    /// Resolves all collected constraints and fills in the concrete types.
    ///
    /// This is the main entry point for constraint solving after all constraints
    /// have been gathered during the type checking phase.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if all constraints are successfully resolved
    /// * `Err` containing the first encountered type error
    pub(super) fn resolve(&mut self) -> Result<(), Box<AnalyzerErrorKind>> {
        // The actual constraint solving implementation will be added here
        Ok(())
    }

    /// Adds a subtyping constraint set: `target_type >: all sub_types`
    ///
    /// This constraint enforces that the target_type is a supertype of all the sub_types.
    ///
    /// # Arguments
    ///
    /// * `target_type` - The type that must be a supertype of all sub_types
    /// * `sub_types` - The types that must be subtypes of target_type
    pub(super) fn add_constraint_subtypes(
        &mut self,
        target_type: &TypedSpan,
        sub_types: &[TypedSpan],
    ) {
        self.constraints.push(Constraint::Subtypes {
            target_type: target_type.clone(),
            sub_types: sub_types.to_vec(),
        });
    }

    /// Adds an equality constraint: `target_type = other_type`
    ///
    /// This constraint enforces that the target_type is equal to other_type.
    ///
    /// # Arguments
    ///
    /// * `target_type` - The type that is expected to be equal to other_type
    /// * `other_type` - The type that is being compared for equality
    pub(super) fn add_constraint_equal(&mut self, target_type: &TypedSpan, other_type: &TypedSpan) {
        self.constraints.push(Constraint::Equal {
            target_type: target_type.clone(),
            other_type: other_type.clone(),
        });
    }

    /// Adds a field access constraint: `outer >: inner.field`
    ///
    /// This enforces that a type has a field with a particular name and type.
    ///
    /// # Arguments
    ///
    /// * `outer` - The expected type of the field
    /// * `field` - The name of the field with its source location
    /// * `inner` - The type containing the field
    pub(super) fn add_constraint_field_access(
        &mut self,
        outer: &TypedSpan,
        field: &str,
        inner: &TypedSpan,
    ) {
        self.constraints.push(Constraint::FieldAccess {
            inner: inner.clone(),
            field: field.to_string(),
            outer: outer.clone(),
        });
    }
}
