use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::{Identifier, TypedSpan},
        types::{registry::TypeKind, subtypes::EnforceError},
    },
    utils::span::OptionalSpanned,
};
use std::mem;

impl TypeRegistry {
    /// Resolves all collected constraints and fills in the concrete types.
    ///
    /// This method iterates through all constraints, checking subtype relationships
    /// and refining unknown types until either all constraints are satisfied or
    /// a constraint cannot be satisfied.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if all constraints are successfully resolved
    /// * `Err(error)` containing the first encountered type error
    pub fn resolve(&mut self) -> Result<(), Box<AnalyzerErrorKind>> {
        loop {
            let mut any_bumped = false;
            let mut field_access_error = None;

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                match self.check_constraint(constraint) {
                    Ok(bumped) => {
                        any_bumped |= bumped;
                    }
                    Err(err) => {
                        // Check if this is a field access error.
                        // Store field access error but continue (it might be resolved later).
                        // Return all other errors immediately.
                        if let AnalyzerErrorKind::InvalidFieldAccess { .. } = *err {
                            if field_access_error.is_none() {
                                field_access_error = Some(err);
                            }
                        } else {
                            self.constraints = constraints;
                            return Err(err);
                        }
                    }
                }
            }

            // Put constraints back.
            self.constraints = constraints;

            // If no bumps occurred and we have a field access error, return it as it will
            // never get resolved.
            if !any_bumped {
                if let Some(err) = field_access_error {
                    return Err(err);
                }
                return Ok(());
            }
        }
    }

    /// Checks if a single constraint is satisfied.
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - The constraint is satisfied, with a boolean indicating if any types were bumped.
    /// * `Err(Box<AnalyzerErrorKind>)` - The constraint failed, with the error.
    fn check_constraint(
        &mut self,
        constraint: &Constraint,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use Constraint::*;
        match constraint {
            Subtype { child, parent } => self.check_subtype_constraint(child, parent),
            FieldAccess {
                inner,
                field,
                outer,
            } => self.check_field_access_constraint(inner, field, outer),
        }
    }

    /// Checks if a subtype constraint is satisfied.
    fn check_subtype_constraint(
        &mut self,
        child: &TypedSpan,
        parent: &TypedSpan,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use EnforceError::*;

        let child_span = child.span.clone();
        let parent_span = parent.span.clone();

        self.enforce_subtype(&child.ty, &parent.ty)
            .map_err(|err| match err {
                InvalidMerge(type1, type2) => {
                    let type1 = TypedSpan::new(type1, child_span);
                    let type2 = TypedSpan::new(type2, parent_span);
                    AnalyzerErrorKind::new_type_merge_fail(&type1, &type2)
                }

                InvalidSubtype(child, parent) => {
                    let child = TypedSpan::new(child, child_span);
                    let parent = TypedSpan::new(parent, parent_span);
                    AnalyzerErrorKind::new_invalid_subtype(&child, &parent)
                }
            })
    }

    /// Checks if a field access constraint is satisfied.
    fn check_field_access_constraint(
        &mut self,
        inner: &TypedSpan,
        field: &Identifier,
        outer: &TypedSpan,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            TypeKind::Adt(name) => self
                .get_product_field_type(name, field)
                .ok_or_else(|| AnalyzerErrorKind::new_invalid_field_access(inner, field))
                .and_then(|field_ty| {
                    self.check_subtype_constraint(
                        &TypedSpan::new(field_ty, inner.span.clone()),
                        outer,
                    )
                }),
            _ => Err(AnalyzerErrorKind::new_invalid_field_access(inner, field)),
        }
    }

    /// Recursively resolves all Unknown types within a type structure.
    ///self.
    /// This method walks through composite types (arrays, tuples, etc.)
    /// and replaces any Unknown types with their concrete inferred types
    /// from the registry.
    ///
    /// # Arguments
    ///
    /// * `ty` - The type to resolve
    ///
    /// # Returns
    ///
    /// A new Type with all Unknown types replaced by their concrete types
    pub(super) fn resolve_type(&self, ty: &Type) -> Type {
        use TypeKind::*;

        let resolved_kind = match &*ty.value {
            Unknown(id) => {
                if let Some(resolved) = self.resolved_unknown.get(id) {
                    return self.resolve_type(&resolved.clone().into());
                } else {
                    return ty.clone();
                }
            }
            Array(elem) => Array(self.resolve_type(elem)),
            Closure(param, ret) => Closure(self.resolve_type(param), self.resolve_type(ret)),
            Tuple(elems) => Tuple(elems.iter().map(|e| self.resolve_type(e)).collect()),
            Map(key, val) => Map(self.resolve_type(key), self.resolve_type(val)),
            Optional(inner) => Optional(self.resolve_type(inner)),
            Stored(inner) => Stored(self.resolve_type(inner)),
            Costed(inner) => Costed(self.resolve_type(inner)),
            // For all other types that don't contain nested types, just clone.
            _ => return ty.clone(),
        };

        OptionalSpanned {
            value: resolved_kind.into(),
            span: ty.span.clone(),
        }
    }
}
