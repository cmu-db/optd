use crate::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::{Identifier, TypedSpan},
        types::Type,
    },
    utils::span::Spanned,
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
    ///
    /// Additionally, if all sub_types are equal, then target_type will be set to that type.
    /// This is useful for inference in contexts like common type determination.
    Subtypes {
        target_type: TypedSpan,
        sub_types: Vec<TypedSpan>,
    },

    /// Field access: `inner.field = outer`
    ///
    /// This constraint enforces that the type `inner` has a field named `field`
    /// with a type that matches `outer`. Used for struct field access.
    FieldAccess {
        inner: Type,
        field: Spanned<Identifier>,
        outer: Type,
    },
}

/// The constraint solver that enforces type rules and resolves unknown types.
///
/// The solver collects constraints during type checking and then resolves them
/// to determine concrete types for all expressions in the program.
#[derive(Debug, Default)]
pub struct Solver {
    /// The set of collected type constraints to be solved
    constraints: Vec<Constraint>,

    /// Maps unknown type IDs to their resolved concrete types
    resolved_unknown: HashMap<usize, Type>,
}

impl Solver {
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
    /// Additionally, if all sub_types are equal, then target_type will be set to that type.
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

    /// Adds a field access constraint: `inner.field = outer`
    ///
    /// This enforces that a type has a field with a particular name and type.
    ///
    /// # Arguments
    ///
    /// * `inner` - The type containing the field
    /// * `field` - The name of the field with its source location
    /// * `outer` - The expected type of the field
    pub(super) fn add_constraint_field_access(
        &mut self,
        inner: Type,
        field: Spanned<Identifier>,
        outer: Type,
    ) {
        self.constraints.push(Constraint::FieldAccess {
            inner,
            field,
            outer,
        });
    }
}
