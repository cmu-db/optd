use super::registry::{Constraint, Type, TypeRegistry};
use crate::analyzer::{errors::AnalyzerErrorKind, hir::TypedSpan};
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
    /// * `Err` containing the first encountered type error
    pub fn resolve(&mut self) -> Result<(), Box<AnalyzerErrorKind>> {
        loop {
            let mut any_bumped = false;
            let mut first_error = None;

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                match self.check_constraint(constraint) {
                    Ok(bumped) => {
                        any_bumped |= bumped;
                    }
                    Err((error, bumped)) => {
                        any_bumped |= bumped;
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }

            // Put constraints back.
            self.constraints = constraints;

            // If no types were bumped, we've reached a fixed point.
            if !any_bumped {
                if let Some(error) = first_error {
                    return Err(error);
                } else {
                    return Ok(());
                }
            }
        }
    }

    /// Checks if a single constraint is satisfied.
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - The constraint is satisfied, with a boolean indicating if any types were bumped.
    /// * `Err((Box<AnalyzerErrorKind>, bool))` - The constraint failed, with the error and a boolean
    ///   indicating if any types were bumped.
    fn check_constraint(
        &mut self,
        constraint: &Constraint,
    ) -> Result<bool, (Box<AnalyzerErrorKind>, bool)> {
        use Constraint::*;

        match constraint {
            Subtype {
                target_type,
                sub_type,
            } => {
                let mut bumped = false;
                let is_subtype = self.is_subtype_infer(&sub_type.ty, &target_type.ty, &mut bumped);

                if !is_subtype && !bumped {
                    let sub_type = &self.materialize_unknown(sub_type);
                    let target_type = &self.materialize_unknown(target_type);
                    let error = AnalyzerErrorKind::new_invalid_subtype(sub_type, target_type);
                    Err((error, bumped))
                } else {
                    Ok(bumped)
                }
            }

            FieldAccess {
                inner,
                field,
                outer,
            } => {
                let inner = &self.materialize_unknown(inner);
                match &inner.ty {
                    Type::Adt(name) => match self.get_product_field_type(name, field) {
                        Some(field_ty) => {
                            let mut bumped = false;
                            let is_subtype =
                                self.is_subtype_infer(&field_ty, &outer.ty, &mut bumped);

                            if !is_subtype && !bumped {
                                let outer = &self.materialize_unknown(outer);
                                let error = AnalyzerErrorKind::new_invalid_subtype(inner, outer);
                                Err((error, bumped))
                            } else {
                                Ok(bumped)
                            }
                        }
                        None => {
                            let outer = &self.materialize_unknown(outer);
                            let error = AnalyzerErrorKind::new_invalid_field_access(
                                inner,
                                field,
                                &outer.span,
                            );
                            Err((error, false))
                        }
                    },
                    _ => {
                        let outer = &self.materialize_unknown(outer);
                        let error =
                            AnalyzerErrorKind::new_invalid_field_access(inner, field, &outer.span);
                        Err((error, false))
                    }
                }
            }
        }
    }

    fn materialize_unknown(&self, typed_span: &TypedSpan) -> TypedSpan {
        match &typed_span.ty {
            Type::Unknown(id) => {
                let resolved_type = self.resolved_unknown.get(id).unwrap().clone();
                TypedSpan::new(resolved_type, typed_span.span.clone())
            }
            _ => typed_span.clone(),
        }
    }
}
