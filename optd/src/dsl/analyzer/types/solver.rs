use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::analyzer::{
    errors::AnalyzerErrorKind,
    hir::{Identifier, TypedSpan},
    types::{registry::TypeKind, subtypes::EnforceError},
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
            any_changed = false;

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                any_changed |= self.check_constraint(constraint)?
            }

            // Put constraints back.
            self.constraints = constraints;
        }

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
                    &child.ty,
                    parent_ty,
                    &child.span,
                    self.resolved_unknown.clone(),
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
                self.resolved_unknown.clone(),
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
                    AnalyzerErrorKind::new_invalid_field_access(
                        &inner_resolved,
                        &inner.span,
                        field,
                        self.resolved_unknown.clone(),
                    )
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
                self.resolved_unknown.clone(),
            )),
        }
    }

    /// Resolves any Unknown types to their concrete types.
    ///
    /// This method checks if a type is an Unknown variant and replaces it
    /// with its concrete inferred type from the registry. Other types are
    /// returned as-is.
    ///
    /// # Arguments
    ///
    /// * `ty` - The type to resolve
    ///
    /// # Returns
    ///
    /// The resolved Type if it was an Unknown type, otherwise the original Type
    pub(super) fn resolve_type(&self, ty: &Type) -> Type {
        use TypeKind::*;

        match &*ty.value {
            UnknownAsc(id) | UnknownDesc(id) => {
                if let Some(resolved) = self.resolved_unknown.get(id) {
                    resolved.clone()
                } else {
                    ty.clone()
                }
            }
            _ => ty.clone(),
        }
    }
}
