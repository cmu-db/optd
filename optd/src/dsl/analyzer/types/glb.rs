use super::registry::{Type, TypeRegistry};
use crate::dsl::analyzer::types::registry::TypeKind;

impl TypeRegistry {
    /// Finds the greatest lower bound (GLB) of two types.
    /// The greatest lower bound is the most general type that is a subtype of both input types.
    ///
    /// The GLB follows these principles:
    /// 1. Primitive types check for equality.
    /// 2. For container types (Array, Tuple), it applies covariance rules in reverse.
    /// 3. For function (and Map) types, it uses contravariance in reverse: covariance for parameters,
    ///    contravariance for return types.
    /// 4. For ADTs, it finds the more specific of the two types if one is a subtype of the other, given
    ///    the absence of multiple inheritance within ADTs.
    /// 5. For wrapper types:
    ///    - Optional(T) and Optional(U) yields Optional(GLB(T, U)).
    ///    - Optional(T) and U yields GLB(T, U).
    ///    - None is the GLB of None and any Optional type.
    ///    - For Stored/Costed: Costed is more specific than Stored, so mixed operations yield Costed.
    /// 6. For compatibility between collections and functions:
    ///    - Map and Function: returns the more specific type if one is a subtype of the other,
    ///      or computes a common Function type if no direct subtyping relationship exists.
    ///    - Array and Function: returns the more specific type if one is a subtype of the other,
    ///      or computes a common Function type if no direct subtyping relationship exists.
    ///      Only applies when the Function parameter type is I64.
    /// 7. Nothing is the bottom type, Universe is the top type, so GLB(Universe, T) = T and GLB(Nothing, T) = Nothing.
    /// 8. If no common subtype exists, returns the Nothing type.
    ///
    /// # Arguments
    ///
    /// * `type1` - First type to compare
    /// * `type2` - Second type to compare
    /// * `has_changed` - Mutable reference to a boolean that will be set to true if
    ///   any unknown types were modified during the computation.
    ///
    /// # Returns
    ///
    /// The greatest lower bound of the two types, or Nothing if no common subtype exists.
    pub(super) fn greatest_lower_bound(
        &mut self,
        type1: &Type,
        type2: &Type,
        has_changed: &mut bool,
    ) -> Type {
        use TypeKind::*;

        let glb_kind = match (&*type1.value, &*type2.value) {
            // Substitute Unknown types with their current inferred type.
            (UnknownAsc(_) | UnknownDesc(_), _) => {
                let bound_unknown = self.resolve_type(type1);
                return self.greatest_lower_bound(&bound_unknown, type2, has_changed);
            }
            (_, UnknownAsc(_) | UnknownDesc(_)) => {
                let bound_unknown = self.resolve_type(type2);
                return self.greatest_lower_bound(type1, &bound_unknown, has_changed);
            }

            // Nothing is the bottom type - GLB with anything is Nothing.
            (Nothing, _) | (_, Nothing) => Nothing,

            // Universe is the top type - GLB(Universe, T) = T.
            (Universe, other) => other.clone(),
            (other, Universe) => other.clone(),

            // Primitive types - check for equality.
            (I64, I64)
            | (String, String)
            | (F64, F64)
            | (Bool, Bool)
            | (Unit, Unit)
            | (None, None) => *type1.value.clone(),

            // Array covariance: GLB(Array<T1>, Array<T2>) = Array<GLB(T1, T2)>.
            (Array(elem1), Array(elem2)) => {
                let glb_elem = self.greatest_lower_bound(elem1, elem2, has_changed);
                Array(glb_elem)
            }

            // Tuple covariance: GLB((T1,T2,...), (U1,U2,...)) = (GLB(T1,U1), GLB(T2,U2), ...).
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let mut glb_elems = Vec::with_capacity(elems1.len());
                for (e1, e2) in elems1.iter().zip(elems2.iter()) {
                    glb_elems.push(self.greatest_lower_bound(e1, e2, has_changed));
                }
                Tuple(glb_elems)
            }

            // Map with contravariant keys and covariant values.
            (Map(key1, val1), Map(key2, val2)) => {
                let lub_key = self.least_upper_bound(key1, key2, has_changed);
                let glb_val = self.greatest_lower_bound(val1, val2, has_changed);
                Map(lub_key, glb_val)
            }

            // Optional type handling.
            (Optional(inner1), Optional(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2, has_changed);
                Optional(glb_inner)
            }
            (None, Optional(_)) | (Optional(_), None) => None,
            (Optional(inner), _) => {
                return self.greatest_lower_bound(inner, type2, has_changed);
            }
            (_, Optional(inner)) => {
                return self.greatest_lower_bound(type1, inner, has_changed);
            }

            // Stored type handling.
            (Stored(inner1), Stored(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2, has_changed);
                Stored(glb_inner)
            }

            // Costed type handling.
            (Costed(inner1), Costed(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2, has_changed);
                Costed(glb_inner)
            }

            // Mixed Stored and Costed - result is Costed (more specific).
            (Costed(costed), Stored(stored)) | (Stored(stored), Costed(costed)) => {
                let glb_inner = self.greatest_lower_bound(costed, stored, has_changed);
                Costed(glb_inner)
            }

            // Unwrap Stored/Costed for GLB with other types.
            (Stored(stored), _) => {
                return self.greatest_lower_bound(stored, type2, has_changed);
            }
            (_, Stored(stored)) => {
                return self.greatest_lower_bound(type1, stored, has_changed);
            }
            (Costed(costed), _) => {
                return self.greatest_lower_bound(costed, type2, has_changed);
            }
            (_, Costed(costed)) => {
                return self.greatest_lower_bound(type1, costed, has_changed);
            }

            // Function type handling - covariant parameters, contravariant return types (reversed from LUB).
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_lub = self.least_upper_bound(param1, param2, has_changed);
                let ret_glb = self.greatest_lower_bound(ret1, ret2, has_changed);
                Closure(param_lub, ret_glb)
            }

            // Map/Function compatibility - a Map can be seen as a function from keys to optional values.
            (Map(key_type, val_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Map(key_type, val_type)) => {
                let param_lub = self.least_upper_bound(param_type, key_type, has_changed);
                let ret_glb = self.greatest_lower_bound(
                    ret_type,
                    &Optional(val_type.clone()).into(),
                    has_changed,
                );

                if let Optional(inner) = &*ret_glb.value {
                    Map(param_lub, inner.clone())
                } else {
                    // Return Nothing for incompatible types
                    Nothing
                }
            }

            // Array/Function compatibility - an Array can be seen as a function from indices to values.
            (Array(elem_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Array(elem_type))
                if matches!(&*param_type.value, I64) =>
            {
                let ret_glb = self.greatest_lower_bound(
                    ret_type,
                    &Optional(elem_type.clone()).into(),
                    has_changed,
                );

                if let Optional(inner) = &*ret_glb.value {
                    Array(inner.clone())
                } else {
                    // Return Nothing for incompatible types
                    Nothing
                }
            }

            // ADT types - find subtype relationship if it exists.
            (Adt(adt1), Adt(adt2)) => {
                if self.inherits_adt(adt1, adt2) {
                    *type1.value.clone()
                } else if self.inherits_adt(adt2, adt1) {
                    *type2.value.clone()
                } else {
                    // Return Nothing for incompatible types
                    Nothing
                }
            }

            // Native trait handling: re-use enforce subtype for convenience.
            (Concat | EqHash | Arithmetic, other)
                if self.is_subtype_infer(type2, type1, has_changed) =>
            {
                other.clone()
            }
            (other, Concat | EqHash | Arithmetic)
                if self.is_subtype_infer(type1, type2, has_changed) =>
            {
                other.clone()
            }

            // Default case - incompatible types
            _ => Nothing,
        };

        let result = glb_kind.into();

        // Verify post-condition in debug mode only
        debug_assert!(
            self.is_subtype(&result, type1),
            "GLB post-condition failed: {:?} is not a subtype of {:?}",
            result,
            type1
        );
        debug_assert!(
            self.is_subtype(&result, type2),
            "GLB post-condition failed: {:?} is not a subtype of {:?}",
            result,
            type2
        );

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::analyzer::types::lub::tests::setup_type_hierarchy;
    use TypeKind::*;

    /// Helper function to simplify GLB assertions
    pub fn assert_glb_eq(reg: &mut TypeRegistry, t1: &Type, t2: &Type, expected: TypeKind) {
        let mut has_changed = false;
        let result = reg.greatest_lower_bound(t1, t2, &mut has_changed);
        assert_eq!(result, expected.into());
    }

    /// Helper function to verify GLB results in Nothing
    pub fn assert_glb_nothing(reg: &mut TypeRegistry, t1: &Type, t2: &Type) {
        let mut has_changed = false;
        let result = reg.greatest_lower_bound(t1, t2, &mut has_changed);
        assert_eq!(*result.value, Nothing);
    }

    #[test]
    fn test_greatest_lower_bound_simple() {
        let mut reg = setup_type_hierarchy();

        // Identical ADT types
        assert_glb_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Dog".to_string()).into(),
            Adt("Dog".to_string()),
        );

        // Subtype/supertype relationship
        assert_glb_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Mammals".to_string()).into(),
            Adt("Dog".to_string()),
        );

        // Related types (siblings)
        assert_glb_nothing(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Cat".to_string()).into(),
        );

        // Unrelated types
        assert_glb_nothing(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Car".to_string()).into(),
        );

        // Universe with ADT type
        assert_glb_eq(
            &mut reg,
            &Universe.into(),
            &Adt("Dog".to_string()).into(),
            Adt("Dog".to_string()),
        );

        // Nothing with ADT type
        assert_glb_eq(
            &mut reg,
            &Nothing.into(),
            &Adt("Dog".to_string()).into(),
            Nothing,
        );
    }

    #[test]
    fn test_greatest_lower_bound_array() {
        let mut reg = setup_type_hierarchy();

        // Arrays of same ADT types
        assert_glb_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Dog".to_string()).into()).into(),
            Array(Adt("Dog".to_string()).into()),
        );

        // Arrays of subtype/supertype relationship
        assert_glb_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Mammals".to_string()).into()).into(),
            Array(Adt("Dog".to_string()).into()),
        );

        // Arrays of related types (siblings)
        assert_glb_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Cat".to_string()).into()).into(),
            Array(Nothing.into()),
        );
    }

    #[test]
    fn test_greatest_lower_bound_tuple() {
        let mut reg = setup_type_hierarchy();

        // Tuples of same ADT types
        assert_glb_eq(
            &mut reg,
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
            Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ]),
        );

        // Tuples with subtype/supertype relationship
        assert_glb_eq(
            &mut reg,
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
            &Tuple(vec![
                Adt("Mammals".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ])
            .into(),
            Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ]),
        );

        // Tuples with mixed compatibility
        assert_glb_eq(
            &mut reg,
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
            &Tuple(vec![
                Adt("Cat".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ])
            .into(),
            Tuple(vec![Nothing.into(), Adt("Car".to_string()).into()]),
        );

        // Different length tuples
        assert_glb_nothing(
            &mut reg,
            &Tuple(vec![Adt("Dog".to_string()).into()]).into(),
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
        );
    }

    #[test]
    fn test_greatest_lower_bound_map() {
        let mut reg = setup_type_hierarchy();

        // Maps with same key and value types
        assert_glb_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Maps with covariant keys (opposite of LUB due to contravariance)
        assert_glb_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            )
            .into(),
            Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            ),
        );

        // Maps with contravariant values (opposite of LUB due to covariance)
        assert_glb_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Dog".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            )
            .into(),
            Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Maps with mixed key and value type relationships
        assert_glb_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Mammals".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            )
            .into(),
            Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            ),
        );
    }

    #[test]
    fn test_greatest_lower_bound_function() {
        let mut reg = setup_type_hierarchy();

        // Functions with same parameter and return types
        assert_glb_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Functions with covariant parameters (opposite of LUB due to contravariance)
        assert_glb_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            )
            .into(),
            Closure(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            ),
        );

        // Functions with contravariant return types (opposite of LUB due to covariance)
        assert_glb_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Dog".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            )
            .into(),
            Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Functions with mixed parameter and return type relationships
        assert_glb_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Mammals".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            )
            .into(),
            Closure(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            ),
        );
    }

    #[test]
    fn test_greatest_lower_bound_optional() {
        let mut reg = setup_type_hierarchy();

        // Optional of same types
        assert_glb_eq(
            &mut reg,
            &Optional(Adt("Dog".to_string()).into()).into(),
            &Optional(Adt("Dog".to_string()).into()).into(),
            Optional(Adt("Dog".to_string()).into()),
        );

        // Optional of subtype/supertype
        assert_glb_eq(
            &mut reg,
            &Optional(Adt("Dog".to_string()).into()).into(),
            &Optional(Adt("Mammals".to_string()).into()).into(),
            Optional(Adt("Dog".to_string()).into()),
        );

        // Optional of unrelated types
        assert_glb_eq(
            &mut reg,
            &Optional(Adt("Dog".to_string()).into()).into(),
            &Optional(Adt("Car".to_string()).into()).into(),
            Optional(Nothing.into()),
        );

        // None and Optional
        assert_glb_eq(
            &mut reg,
            &None.into(),
            &Optional(Adt("Dog".to_string()).into()).into(),
            None,
        );

        // None and None
        assert_glb_eq(&mut reg, &None.into(), &None.into(), None);

        // Optional and base type
        assert_glb_eq(
            &mut reg,
            &Optional(Adt("Mammals".to_string()).into()).into(),
            &Adt("Dog".to_string()).into(),
            Adt("Dog".to_string()),
        );
    }

    #[test]
    fn test_greatest_lower_bound_stored_costed() {
        let mut reg = setup_type_hierarchy();

        // Stored of same types
        assert_glb_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Stored(Adt("Dog".to_string()).into()).into(),
            Stored(Adt("Dog".to_string()).into()),
        );

        // Stored of subtype/supertype
        assert_glb_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Stored(Adt("Mammals".to_string()).into()).into(),
            Stored(Adt("Dog".to_string()).into()),
        );

        // Costed of same types
        assert_glb_eq(
            &mut reg,
            &Costed(Adt("Dog".to_string()).into()).into(),
            &Costed(Adt("Dog".to_string()).into()).into(),
            Costed(Adt("Dog".to_string()).into()),
        );

        // Mixed Stored and Costed (Costed is more specific than Stored)
        assert_glb_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Costed(Adt("Dog".to_string()).into()).into(),
            Costed(Adt("Dog".to_string()).into()),
        );

        // Mixed with subtype/supertype relationship
        assert_glb_eq(
            &mut reg,
            &Stored(Adt("Mammals".to_string()).into()).into(),
            &Costed(Adt("Dog".to_string()).into()).into(),
            Costed(Adt("Dog".to_string()).into()),
        );
    }

    #[test]
    fn test_greatest_lower_bound_collection_function() {
        let mut reg = setup_type_hierarchy();

        // Map and Function compatibility
        assert_glb_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Mammals".to_string()).into(),
                Optional(Adt("LandVehicles".to_string()).into()).into(),
            )
            .into(),
            Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            ),
        );

        // Array and Function compatibility
        assert_glb_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Closure(
                I64.into(),
                Optional(Adt("Mammals".to_string()).into()).into(),
            )
            .into(),
            Array(Adt("Dog".to_string()).into()),
        );
    }

    #[test]
    fn test_greatest_lower_bound_complex_nested_types() {
        let mut reg = setup_type_hierarchy();

        // Array of Optional ADTs
        assert_glb_eq(
            &mut reg,
            &Array(Optional(Adt("Mammals".to_string()).into()).into()).into(),
            &Array(Optional(Adt("Dog".to_string()).into()).into()).into(),
            Array(Optional(Adt("Dog".to_string()).into()).into()),
        );

        // Map with ADT keys and function values
        assert_glb_eq(
            &mut reg,
            &Map(
                Adt("Dog".to_string()).into(),
                Closure(
                    Adt("Car".to_string()).into(),
                    Adt("Boat".to_string()).into(),
                )
                .into(),
            )
            .into(),
            &Map(
                Adt("Animals".to_string()).into(),
                Closure(
                    Adt("LandVehicles".to_string()).into(),
                    Adt("WaterVehicles".to_string()).into(),
                )
                .into(),
            )
            .into(),
            Map(
                Adt("Animals".to_string()).into(), // LUB of keys
                Closure(
                    Adt("LandVehicles".to_string()).into(), // LUB of function parameters
                    Adt("Boat".to_string()).into(),         // GLB of function returns
                )
                .into(),
            ),
        );

        // Optional Stored Tuple of ADTs
        assert_glb_eq(
            &mut reg,
            &Optional(
                Stored(
                    Tuple(vec![
                        Adt("Mammals".to_string()).into(),
                        Adt("LandVehicles".to_string()).into(),
                    ])
                    .into(),
                )
                .into(),
            )
            .into(),
            &Optional(
                Stored(
                    Tuple(vec![
                        Adt("Dog".to_string()).into(),
                        Adt("Car".to_string()).into(),
                    ])
                    .into(),
                )
                .into(),
            )
            .into(),
            Optional(
                Stored(
                    Tuple(vec![
                        Adt("Dog".to_string()).into(),
                        Adt("Car".to_string()).into(),
                    ])
                    .into(),
                )
                .into(),
            ),
        );
    }
}
