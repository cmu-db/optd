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
        let mut any_changed = true;

        while any_changed {
            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                any_changed |= self.check_constraint(constraint)?
            }

            // Put constraints back.
            self.constraints = constraints;
        }

        // TODO: Here we should check if everything has been inferred
        // (i.e. no remaining Nothings).

        Ok(())
    }

    /// Checks if a single constraint is satisfied.
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - The constraint is satisfied, with a boolean indicating if any types were changed.
    /// * `Err(Box<AnalyzerErrorKind>)` - The constraint failed, with the error.
    fn check_constraint(
        &mut self,
        constraint: &Constraint,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use Constraint::*;

        match constraint {
            Subtype { child, parent } => self.check_subtype_constraint(child, parent),
            Call { inner, args, outer } => self.check_call_constraint(inner, args, outer),
            FieldAccess {
                inner,
                field,
                outer,
            } => self.check_field_access_constraint(inner, field, outer),
        }
    }

    fn check_subtype_constraint(
        &mut self,
        child: &TypedSpan,
        parent_ty: &Type,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use EnforceError::*;

        self.enforce_subtype(&child.ty, parent_ty).map_err(|err| {
            match err {
                // For now, no need to distinguish between both errors.
                // However, in the future we might want to do something fancier here to
                // improve error reporting.
                Merge | Meet | Subtype => AnalyzerErrorKind::new_invalid_subtype(
                    &self.resolve_type(&child.ty),
                    &self.resolve_type(parent_ty),
                    &child.span,
                ),
            }
        })
    }

    fn check_call_constraint(
        &mut self,
        inner: &TypedSpan,
        args: &[TypedSpan],
        outer: &TypedSpan,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use TypeKind::*;

        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            Nothing => Ok(false),

            Closure(param, ret) => {
                let param_len = match &**param {
                    Tuple(types) => types.len(),
                    Unit => 0,
                    _ => 1,
                };

                if param_len != args.len() {
                    return Err(AnalyzerErrorKind::new_argument_number_mismatch(
                        &inner.span,
                        param_len,
                        args.len(),
                    ));
                }

                let param_types = match &**param {
                    Tuple(types) => types.to_vec(),
                    Unit => vec![],
                    _ => vec![param.clone()],
                };

                let mut param_changed = false;
                for (arg, param_type) in args.iter().zip(param_types.iter()) {
                    let changed = self.check_subtype_constraint(arg, param_type)?;
                    param_changed |= changed;
                }

                let ret_changed = self.check_subtype_constraint(
                    &TypedSpan::new(ret.clone(), inner.span.clone()),
                    &outer.ty,
                )?;

                Ok(param_changed || ret_changed)
            }

            Map(key_type, val_type) => {
                if args.len() != 1 {
                    return Err(AnalyzerErrorKind::new_argument_number_mismatch(
                        &inner.span,
                        1,
                        args.len(),
                    ));
                }

                self.check_subtype_constraint(&args[0], key_type)?;

                let optional_val_type = Optional(val_type.clone()).into();
                self.check_subtype_constraint(
                    &TypedSpan::new(optional_val_type, inner.span.clone()),
                    &outer.ty,
                )
            }

            Array(elem_type) => {
                if args.len() != 1 {
                    return Err(AnalyzerErrorKind::new_argument_number_mismatch(
                        &inner.span,
                        1,
                        args.len(),
                    ));
                }

                self.check_subtype_constraint(&args[0], &I64.into())?;

                let optional_elem_type = Optional(elem_type.clone()).into();
                self.check_subtype_constraint(
                    &TypedSpan::new(optional_elem_type, inner.span.clone()),
                    &outer.ty,
                )
            }
            _ => Err(AnalyzerErrorKind::new_invalid_call_receiver(
                &inner_resolved,
                &inner.span,
            )),
        }
    }

    fn check_field_access_constraint(
        &mut self,
        inner: &TypedSpan,
        field: &Identifier,
        outer: &Type,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use TypeKind::*;

        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            Nothing => Ok(false),

            Adt(name) => self
                .get_product_field_type(name, field)
                .ok_or_else(|| {
                    AnalyzerErrorKind::new_invalid_field_access(&inner_resolved, &inner.span, field)
                })
                .and_then(|field_ty| {
                    self.check_subtype_constraint(
                        &TypedSpan::new(field_ty, inner.span.clone()),
                        outer,
                    )
                }),
            _ => Err(AnalyzerErrorKind::new_invalid_field_access(
                &inner_resolved,
                &inner.span,
                field,
            )),
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
                    return self.resolve_type(resolved);
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
