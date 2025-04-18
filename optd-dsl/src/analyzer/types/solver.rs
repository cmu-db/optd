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
            let constraints = mem::take(&mut self.constraints);

            let (any_bumped, first_error) = constraints
                .iter()
                .map(|constraint| self.check_constraint(constraint))
                .fold((false, None), |(mut acc_bumped, acc_err), res| match res {
                    Ok(bumped) => {
                        acc_bumped |= bumped;
                        (acc_bumped, acc_err)
                    }
                    Err((err, bumped)) => {
                        acc_bumped |= bumped;
                        let acc_err = acc_err.or(Some(err));
                        (acc_bumped, acc_err)
                    }
                });

            self.constraints = constraints;

            if !any_bumped {
                return first_error.map_or(Ok(()), Err);
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
                    let sub = self.materialize_unknown(sub_type);
                    let target = self.materialize_unknown(target_type);
                    Err((
                        AnalyzerErrorKind::new_invalid_subtype(&sub, &target),
                        bumped,
                    ))
                } else {
                    Ok(bumped)
                }
            }

            FieldAccess {
                inner,
                field,
                outer,
            } => {
                let inner = self.materialize_unknown(inner);

                match &inner.ty {
                    Type::Adt(name) => match self.get_product_field_type(name, field) {
                        Some(field_ty) => {
                            let mut bumped = false;
                            let is_subtype =
                                self.is_subtype_infer(&field_ty, &outer.ty, &mut bumped);

                            if !is_subtype && !bumped {
                                let outer = self.materialize_unknown(outer);
                                Err((
                                    AnalyzerErrorKind::new_invalid_subtype(&inner, &outer),
                                    bumped,
                                ))
                            } else {
                                Ok(bumped)
                            }
                        }
                        None => {
                            let outer = self.materialize_unknown(outer);
                            Err((
                                AnalyzerErrorKind::new_invalid_field_access(
                                    &inner,
                                    field,
                                    &outer.span,
                                ),
                                false,
                            ))
                        }
                    },
                    _ => {
                        let outer = self.materialize_unknown(outer);
                        Err((
                            AnalyzerErrorKind::new_invalid_field_access(&inner, field, &outer.span),
                            false,
                        ))
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
