use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::analyzer::{errors::AnalyzerErrorKind, hir::TypedSpan};
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
    /// * `Vec<Err>` containing all the encountered type errors
    pub fn resolve(&mut self) -> Result<(), Vec<Box<AnalyzerErrorKind>>> {
        loop {
            let mut any_bumped = false;
            let mut errors = Vec::new();

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                match self.check_constraint(constraint) {
                    Ok(bumped) => {
                        any_bumped |= bumped;
                    }
                    Err((err, bumped)) => {
                        any_bumped |= bumped;
                        errors.push(err);
                    }
                }
            }

            // Put constraints back.
            self.constraints = constraints;

            if !any_bumped {
                return if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors)
                };
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

    /// Recursively resolves all Unknown types within a type structure.
    ///
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
        use Type::*;

        match ty {
            Unknown(id) => {
                if let Some(resolved) = self.resolved_unknown.get(id) {
                    self.resolve_type(resolved)
                } else {
                    ty.clone()
                }
            }
            Array(elem) => Array(self.resolve_type(elem).into()),
            Closure(param, ret) => Closure(
                self.resolve_type(param).into(),
                self.resolve_type(ret).into(),
            ),
            Tuple(elems) => Tuple(elems.iter().map(|e| self.resolve_type(e)).collect()),
            Map(key, val) => Map(self.resolve_type(key).into(), self.resolve_type(val).into()),
            Optional(inner) => Optional(self.resolve_type(inner).into()),
            Stored(inner) => Stored(self.resolve_type(inner).into()),
            Costed(inner) => Costed(self.resolve_type(inner).into()),

            // For all other types that don't contain nested types, just clone.
            _ => ty.clone(),
        }
    }

    fn materialize_unknown(&self, typed_span: &TypedSpan) -> TypedSpan {
        let resolved_ty = self.resolve_type(&typed_span.ty);
        TypedSpan::new(resolved_ty, typed_span.span.clone())
    }
}
