use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::{Identifier, TypedSpan},
        types::registry::TypeKind,
    },
    utils::span::Span,
};
use std::mem;

/// Result type for constraint checking that tracks type changes even on error
type ConstraintResult = Result<bool, (Box<AnalyzerErrorKind>, bool)>;

impl TypeRegistry {
    /// Resolves all collected constraints and fills in the concrete types.
    ///
    /// This method iterates through all constraints, checking subtype relationships
    /// and refining unknown types until either all constraints are satisfied or
    /// a constraint cannot be satisfied and no more progress can be made.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if all constraints are successfully resolved
    /// * `Err(error)` containing the last encountered type error when no further progress could be made
    pub fn resolve(&mut self) -> Result<(), Box<AnalyzerErrorKind>> {
        let mut any_changed = true;
        let mut last_error = None;

        while any_changed {
            any_changed = false;
            last_error = None;

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                match self.check_constraint(constraint) {
                    Ok(changed) => {
                        any_changed |= changed;
                    }
                    Err((err, changed)) => {
                        // Store the error but continue processing other constraints.
                        any_changed |= changed;
                        last_error = Some(err);
                    }
                }
            }

            // Put constraints back.
            self.constraints = constraints;
        }

        // Only return an error if no more progress can be made and we have an error.
        match last_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Checks if a single constraint is satisfied.
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - The constraint is satisfied, with a boolean indicating if any types were changed.
    /// * `Err((Box<AnalyzerErrorKind>, bool))` - The constraint failed, with the error and a boolean
    ///    indicating if any types were changed during the check.
    fn check_constraint(&mut self, constraint: &Constraint) -> ConstraintResult {
        match constraint {
            Constraint::Subtype { child, parent } => self.check_subtype_constraint(child, parent),

            Constraint::Call { inner, args, outer } => {
                self.check_call_constraint(inner, args, outer)
            }

            Constraint::FieldAccess {
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
    ) -> ConstraintResult {
        let mut has_changed = false;
        let is_subtype = self.is_subtype_infer(&child.ty, parent_ty, &mut has_changed);

        if is_subtype {
            Ok(has_changed)
        } else {
            let err = AnalyzerErrorKind::new_invalid_subtype(
                &child.ty,
                parent_ty,
                &child.span,
                self.resolved_unknown.clone(),
            );
            Err((err, has_changed))
        }
    }

    fn check_call_constraint(
        &mut self,
        inner: &TypedSpan,
        args: &[TypedSpan],
        outer: &TypedSpan,
    ) -> ConstraintResult {
        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            TypeKind::Nothing => Ok(false),

            TypeKind::Closure(param, ret) => {
                let (param_len, param_types) = match &**param {
                    TypeKind::Tuple(types) => (types.len(), types.to_vec()),
                    TypeKind::Unit => (0, vec![]),
                    _ => (1, vec![param.clone()]),
                };

                if param_len != args.len() {
                    return Err((
                        AnalyzerErrorKind::new_argument_number_mismatch(
                            &inner.span,
                            param_len,
                            args.len(),
                        ),
                        false,
                    ));
                }

                let param_result = args.iter().zip(param_types.iter()).try_fold(
                    false,
                    |acc_changes, (arg, param_type)| match self
                        .check_subtype_constraint(arg, param_type)
                    {
                        Ok(changed) => Ok(acc_changes | changed),
                        Err((err, changed)) => Err((err, acc_changes | changed)),
                    },
                );
                let ret_result = self.check_subtype_constraint(
                    &TypedSpan::new(ret.clone(), inner.span.clone()),
                    &outer.ty,
                );

                self.combine_results(param_result, ret_result)
            }

            TypeKind::Map(key_type, val_type) => {
                self.check_indexable(&inner.span, args, key_type, val_type, &outer.ty)
            }

            TypeKind::Array(elem_type) => {
                let index_type = TypeKind::I64.into();
                self.check_indexable(&inner.span, args, &index_type, elem_type, &outer.ty)
            }

            _ => Err((
                AnalyzerErrorKind::new_invalid_call_receiver(
                    &inner_resolved,
                    &inner.span,
                    self.resolved_unknown.clone(),
                ),
                false,
            )),
        }
    }

    fn check_indexable(
        &mut self,
        span: &Span,
        args: &[TypedSpan],
        key_type: &Type,
        elem_type: &Type,
        outer_ty: &Type,
    ) -> ConstraintResult {
        if args.len() != 1 {
            return Err((
                AnalyzerErrorKind::new_argument_number_mismatch(span, 1, args.len()),
                false,
            ));
        }

        let index_result = self.check_subtype_constraint(&args[0], key_type);
        let optional_elem_type = TypeKind::Optional(elem_type.clone()).into();
        let elem_result = self
            .check_subtype_constraint(&TypedSpan::new(optional_elem_type, span.clone()), outer_ty);

        self.combine_results(index_result, elem_result)
    }

    fn combine_results(
        &self,
        result1: ConstraintResult,
        result2: ConstraintResult,
    ) -> ConstraintResult {
        match (result1, result2) {
            (Ok(changed1), Ok(changed2)) => Ok(changed1 | changed2),
            (Ok(changed1), Err((err2, changed2))) => Err((err2, changed1 | changed2)),
            (Err((err1, changed1)), Ok(changed2)) => Err((err1, changed1 | changed2)),
            (Err((err1, changed1)), Err((_, changed2))) => Err((err1, changed1 | changed2)),
        }
    }

    fn check_field_access_constraint(
        &mut self,
        inner: &TypedSpan,
        field: &Identifier,
        outer: &Type,
    ) -> ConstraintResult {
        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            TypeKind::Nothing => Ok(false),

            TypeKind::Adt(name) => {
                match self.get_product_field_type(name, field) {
                    Some(field_ty) => {
                        // Check that the field type is a subtype of the outer type.
                        self.check_subtype_constraint(
                            &TypedSpan::new(field_ty, inner.span.clone()),
                            outer,
                        )
                    }
                    None => Err((
                        AnalyzerErrorKind::new_invalid_field_access(
                            &inner_resolved,
                            &inner.span,
                            field,
                            self.resolved_unknown.clone(),
                        ),
                        false,
                    )),
                }
            }

            _ => Err((
                AnalyzerErrorKind::new_invalid_field_access(
                    &inner_resolved,
                    &inner.span,
                    field,
                    self.resolved_unknown.clone(),
                ),
                false,
            )),
        }
    }
}
