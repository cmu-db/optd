use std::mem;

use super::registry::{Constraint, Type, TypeRegistry};
use crate::analyzer::errors::AnalyzerErrorKind;

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
            let mut all_satisfied = true;

            // Temporarily take ownership of constraints, to circumvent Rust's
            // overly conservative borrow-checker.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                let (constraint_satisfied, constraint_bumped) = self.check_constraint(constraint);
                any_bumped = any_bumped || constraint_bumped;
                all_satisfied = all_satisfied && constraint_satisfied;
            }

            // Put constraints back.
            self.constraints = constraints;

            if !any_bumped {
                if all_satisfied {
                    return Ok(());
                } else {
                    panic!("Unsolvable type constraint found");
                }
            }
        }
    }

    /// Checks if a single constraint is satisfied.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - Whether the constraint is satisfied (true) or not (false).
    /// - Whether any types were refined (bumped) during the check.
    fn check_constraint(&mut self, constraint: &Constraint) -> (bool, bool) {
        use Constraint::*;

        match constraint {
            Subtype {
                target_type,
                sub_type,
            } => {
                let mut bumped = false;
                let is_subtype = self.is_subtype_infer(&sub_type.ty, &target_type.ty, &mut bumped);
                (is_subtype, bumped)
            }
            FieldAccess {
                inner,
                field,
                outer,
            } => {
                let inner_ty = if let Type::Unknown(id) = &inner.ty {
                    self.resolved_unknown.get(id).unwrap()
                } else {
                    &inner.ty
                };

                if let Type::Adt(name) = inner_ty {
                    self.get_product_field_type(name, field)
                        .map(|ty| {
                            let mut bumped = false;
                            let is_subtype = self.is_subtype_infer(&ty, &outer.ty, &mut bumped);
                            (is_subtype, bumped)
                        })
                        .unwrap_or((false, false))
                } else {
                    (false, false)
                }
            }
        }
    }
}
