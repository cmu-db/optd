use super::registry::TypeRegistry;
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
    /// 8. If no common subtype exists, Nothing is returned as the fallback.
    ///
    /// # Arguments
    ///
    /// * `type1` - The first type
    /// * `type2` - The second type
    ///
    /// # Returns
    ///
    /// The greatest lower bound of the two types. Returns `Type::Nothing` if no common
    /// subtype exists.
    pub(super) fn greatest_lower_bound(&mut self, type1: &TypeKind, type2: &TypeKind) -> TypeKind {
        use TypeKind::*;

        let glb = match (type1, type2) {
            // Substitute Unknown types with their current inferred type.
            (unknown @ Unknown(_), other) | (other, unknown @ Unknown(_)) => {
                let bound_unknown = self.resolve_type(&unknown.clone().into());
                self.least_upper_bound(&bound_unknown, other)
            }

            // Universe is the top type - GLB(Universe, T) = T.
            (Universe, other) | (other, Universe) => other.clone(),

            // Primitive types - check for equality.
            (I64, I64)
            | (String, String)
            | (F64, F64)
            | (Bool, Bool)
            | (Unit, Unit)
            | (None, None) => type1.clone(),

            // Array covariance: GLB(Array<T1>, Array<T2>) = Array<GLB(T1, T2)>.
            (Array(elem1), Array(elem2)) => {
                let glb_elem = self.greatest_lower_bound(elem1, elem2);
                Array(glb_elem.into())
            }

            // Tuple covariance: GLB((T1,T2,...), (U1,U2,...)) = (GLB(T1,U1), GLB(T2,U2), ...).
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let glb_elems: Vec<_> = elems1
                    .iter()
                    .zip(elems2.iter())
                    .map(|(e1, e2)| self.greatest_lower_bound(e1, e2).into())
                    .collect();

                Tuple(glb_elems)
            }

            // Map with contravariant keys and covariant values.
            (Map(key1, val1), Map(key2, val2)) => {
                let lub_key = self.least_upper_bound(key1, key2);
                let glb_val = self.greatest_lower_bound(val1, val2);

                Map(lub_key.into(), glb_val.into())
            }

            // Optional type handling.
            (Optional(inner1), Optional(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                Optional(glb_inner.into())
            }
            (None, Optional(_)) | (Optional(_), None) => None,
            (Optional(inner), other) | (other, Optional(inner)) => {
                self.greatest_lower_bound(inner, other)
            }

            // Stored type handling.
            (Stored(inner1), Stored(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                Stored(glb_inner.into())
            }

            // Costed type handling.
            (Costed(inner1), Costed(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                Costed(glb_inner.into())
            }

            // Mixed Stored and Costed - result is Costed (more specific).
            (Costed(costed), Stored(stored)) | (Stored(stored), Costed(costed)) => {
                let glb_inner = self.greatest_lower_bound(costed, stored);
                Costed(glb_inner.into())
            }

            // Function type handling - covariant parameters, contravariant return types (reversed from LUB).
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_type = self.least_upper_bound(param1, param2);
                let ret_type = self.greatest_lower_bound(ret1, ret2);

                Closure(param_type.into(), ret_type.into())
            }

            // Map/Function compatibility - a Map can be seen as a function from keys to optional values.
            (Map(key_type, val_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Map(key_type, val_type)) => {
                let param_lub = self.least_upper_bound(param_type, key_type);
                if let Optional(ret_glb) =
                    self.greatest_lower_bound(ret_type, &Optional(val_type.clone()))
                {
                    Map(param_lub.into(), ret_glb)
                } else {
                    Nothing
                }
            }

            // Array/Function compatibility - an Array can be seen as a function from indices to values.
            (Array(elem_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Array(elem_type)) => {
                if matches!(&**param_type, I64) {
                    if let Optional(ret_glb) =
                        self.greatest_lower_bound(ret_type, &Optional(elem_type.clone()))
                    {
                        Array(ret_glb)
                    } else {
                        Nothing
                    }
                } else {
                    Nothing
                }
            }

            // ADT types - find subtype relationship if it exists.
            (Adt(adt1), Adt(adt2)) => {
                if self.inherits_adt(adt1, adt2) {
                    type1.clone()
                } else if self.inherits_adt(adt2, adt1) {
                    type2.clone()
                } else {
                    Nothing
                }
            }

            // Native trait handling: re-use enforce subtype for convenience.
            (trait_type @ (Concat | EqHash | Arithmetic), other)
            | (other, trait_type @ (Concat | EqHash | Arithmetic))
                if self
                    .enforce_subtype(&other.clone().into(), &trait_type.clone().into())
                    .is_ok() =>
            {
                other.clone()
            }

            // Default case - return Nothing if no common subtype exists.
            _ => Nothing,
        };

        // Verify post-condition.
        assert!(
            self.enforce_subtype(&glb.clone().into(), &type1.clone().into())
                .is_ok()
        );
        assert!(
            self.enforce_subtype(&glb.clone().into(), &type2.clone().into())
                .is_ok()
        );

        glb
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::analyzer::types::lub::tests::setup_type_hierarchy;
    use TypeKind::*;

    #[test]
    fn test_greatest_lower_bound_simple() {
        let mut reg = setup_type_hierarchy();

        // Identical ADT types
        assert_eq!(
            reg.greatest_lower_bound(&Adt("Dog".to_string()), &Adt("Dog".to_string())),
            Adt("Dog".to_string())
        );

        // Subtype/supertype relationship
        assert_eq!(
            reg.greatest_lower_bound(&Adt("Dog".to_string()), &Adt("Mammals".to_string())),
            Adt("Dog".to_string())
        );

        // Related types (siblings)
        assert_eq!(
            reg.greatest_lower_bound(&Adt("Dog".to_string()), &Adt("Cat".to_string())),
            Nothing
        );

        // Unrelated types
        assert_eq!(
            reg.greatest_lower_bound(&Adt("Dog".to_string()), &Adt("Car".to_string())),
            Nothing
        );

        // Universe with ADT type
        assert_eq!(
            reg.greatest_lower_bound(&Universe, &Adt("Dog".to_string())),
            Adt("Dog".to_string())
        );

        // Nothing with ADT type
        assert_eq!(
            reg.greatest_lower_bound(&Nothing, &Adt("Dog".to_string())),
            Nothing
        );
    }

    #[test]
    fn test_greatest_lower_bound_array() {
        let mut reg = setup_type_hierarchy();

        // Arrays of same ADT types
        assert_eq!(
            reg.greatest_lower_bound(
                &Array(Adt("Dog".to_string()).into()),
                &Array(Adt("Dog".to_string()).into())
            ),
            Array(Adt("Dog".to_string()).into())
        );

        // Arrays of subtype/supertype relationship
        assert_eq!(
            reg.greatest_lower_bound(
                &Array(Adt("Dog".to_string()).into()),
                &Array(Adt("Mammals".to_string()).into())
            ),
            Array(Adt("Dog".to_string()).into())
        );

        // Arrays of related types (siblings)
        assert_eq!(
            reg.greatest_lower_bound(
                &Array(Adt("Dog".to_string()).into()),
                &Array(Adt("Cat".to_string()).into())
            ),
            Array(Nothing.into())
        );
    }

    #[test]
    fn test_greatest_lower_bound_tuple() {
        let mut reg = setup_type_hierarchy();

        // Tuples of same ADT types
        assert_eq!(
            reg.greatest_lower_bound(
                &Tuple(vec![
                    Adt("Dog".to_string()).into(),
                    Adt("Car".to_string()).into()
                ]),
                &Tuple(vec![
                    Adt("Dog".to_string()).into(),
                    Adt("Car".to_string()).into()
                ])
            ),
            Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into()
            ])
        );

        // Tuples with subtype/supertype relationship
        assert_eq!(
            reg.greatest_lower_bound(
                &Tuple(vec![
                    Adt("Dog".to_string()).into(),
                    Adt("Car".to_string()).into()
                ]),
                &Tuple(vec![
                    Adt("Mammals".to_string()).into(),
                    Adt("LandVehicles".to_string()).into()
                ])
            ),
            Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into()
            ])
        );

        // Tuples with mixed compatibility
        assert_eq!(
            reg.greatest_lower_bound(
                &Tuple(vec![
                    Adt("Dog".to_string()).into(),
                    Adt("Car".to_string()).into()
                ]),
                &Tuple(vec![
                    Adt("Cat".to_string()).into(),
                    Adt("LandVehicles".to_string()).into()
                ])
            ),
            Tuple(vec![Nothing.into(), Adt("Car".to_string()).into()])
        );

        // Different length tuples
        assert_eq!(
            reg.greatest_lower_bound(
                &Tuple(vec![Adt("Dog".to_string()).into()]),
                &Tuple(vec![
                    Adt("Dog".to_string()).into(),
                    Adt("Car".to_string()).into()
                ])
            ),
            Nothing
        );
    }

    #[test]
    fn test_greatest_lower_bound_map() {
        let mut reg = setup_type_hierarchy();

        // Maps with same key and value types
        assert_eq!(
            reg.greatest_lower_bound(
                &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into())
            ),
            Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into())
        );

        // Maps with covariant keys (opposite of LUB due to contravariance)
        assert_eq!(
            reg.greatest_lower_bound(
                &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Map(
                    Adt("Mammals".to_string()).into(),
                    Adt("Car".to_string()).into()
                )
            ),
            Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into()
            )
        );

        // Maps with contravariant values (opposite of LUB due to covariance)
        assert_eq!(
            reg.greatest_lower_bound(
                &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Map(
                    Adt("Dog".to_string()).into(),
                    Adt("LandVehicles".to_string()).into()
                )
            ),
            Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into())
        );

        // Maps with mixed key and value type relationships
        assert_eq!(
            reg.greatest_lower_bound(
                &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Map(
                    Adt("Mammals".to_string()).into(),
                    Adt("LandVehicles".to_string()).into()
                )
            ),
            Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into()
            )
        );
    }

    #[test]
    fn test_greatest_lower_bound_function() {
        let mut reg = setup_type_hierarchy();

        // Functions with same parameter and return types
        assert_eq!(
            reg.greatest_lower_bound(
                &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into())
            ),
            Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into())
        );

        // Functions with covariant parameters (opposite of LUB due to contravariance)
        assert_eq!(
            reg.greatest_lower_bound(
                &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Closure(
                    Adt("Mammals".to_string()).into(),
                    Adt("Car".to_string()).into()
                )
            ),
            Closure(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into()
            )
        );

        // Functions with contravariant return types (opposite of LUB due to covariance)
        assert_eq!(
            reg.greatest_lower_bound(
                &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Closure(
                    Adt("Dog".to_string()).into(),
                    Adt("LandVehicles".to_string()).into()
                )
            ),
            Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into())
        );

        // Functions with mixed parameter and return type relationships
        assert_eq!(
            reg.greatest_lower_bound(
                &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Closure(
                    Adt("Mammals".to_string()).into(),
                    Adt("LandVehicles".to_string()).into()
                )
            ),
            Closure(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into()
            )
        );
    }

    #[test]
    fn test_greatest_lower_bound_optional() {
        let mut reg = setup_type_hierarchy();

        // Optional of same types
        assert_eq!(
            reg.greatest_lower_bound(
                &Optional(Adt("Dog".to_string()).into()),
                &Optional(Adt("Dog".to_string()).into())
            ),
            Optional(Adt("Dog".to_string()).into())
        );

        // Optional of subtype/supertype
        assert_eq!(
            reg.greatest_lower_bound(
                &Optional(Adt("Dog".to_string()).into()),
                &Optional(Adt("Mammals".to_string()).into())
            ),
            Optional(Adt("Dog".to_string()).into())
        );

        // Optional of unrelated types
        assert_eq!(
            reg.greatest_lower_bound(
                &Optional(Adt("Dog".to_string()).into()),
                &Optional(Adt("Car".to_string()).into())
            ),
            Optional(Nothing.into())
        );

        // None and Optional
        assert_eq!(
            reg.greatest_lower_bound(&None, &Optional(Adt("Dog".to_string()).into())),
            None
        );

        // None and None
        assert_eq!(reg.greatest_lower_bound(&None, &None), None);

        // Optional and base type
        assert_eq!(
            reg.greatest_lower_bound(
                &Optional(Adt("Mammals".to_string()).into()),
                &Adt("Dog".to_string())
            ),
            Adt("Dog".to_string())
        );
    }

    #[test]
    fn test_greatest_lower_bound_stored_costed() {
        let mut reg = setup_type_hierarchy();

        // Stored of same types
        assert_eq!(
            reg.greatest_lower_bound(
                &Stored(Adt("Dog".to_string()).into()),
                &Stored(Adt("Dog".to_string()).into())
            ),
            Stored(Adt("Dog".to_string()).into())
        );

        // Stored of subtype/supertype
        assert_eq!(
            reg.greatest_lower_bound(
                &Stored(Adt("Dog".to_string()).into()),
                &Stored(Adt("Mammals".to_string()).into())
            ),
            Stored(Adt("Dog".to_string()).into())
        );

        // Costed of same types
        assert_eq!(
            reg.greatest_lower_bound(
                &Costed(Adt("Dog".to_string()).into()),
                &Costed(Adt("Dog".to_string()).into())
            ),
            Costed(Adt("Dog".to_string()).into())
        );

        // Mixed Stored and Costed (Costed is more specific than Stored)
        assert_eq!(
            reg.greatest_lower_bound(
                &Stored(Adt("Dog".to_string()).into()),
                &Costed(Adt("Dog".to_string()).into())
            ),
            Costed(Adt("Dog".to_string()).into())
        );

        // Mixed with subtype/supertype relationship
        assert_eq!(
            reg.greatest_lower_bound(
                &Stored(Adt("Mammals".to_string()).into()),
                &Costed(Adt("Dog".to_string()).into())
            ),
            Costed(Adt("Dog".to_string()).into())
        );
    }

    #[test]
    fn test_greatest_lower_bound_collection_function() {
        let mut reg = setup_type_hierarchy();

        // Map and Function compatibility
        assert_eq!(
            reg.greatest_lower_bound(
                &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
                &Closure(
                    Adt("Mammals".to_string()).into(),
                    Optional(Adt("LandVehicles".to_string()).into()).into()
                )
            ),
            Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into()
            )
        );

        // Array and Function compatibility
        assert_eq!(
            reg.greatest_lower_bound(
                &Array(Adt("Dog".to_string()).into()),
                &Closure(
                    I64.into(),
                    Optional(Adt("Mammals".to_string()).into()).into()
                )
            ),
            Array(Adt("Dog".to_string()).into())
        );
    }

    #[test]
    fn test_greatest_lower_bound_complex_nested_types() {
        let mut reg = setup_type_hierarchy();

        // Array of Optional ADTs
        assert_eq!(
            reg.greatest_lower_bound(
                &Array(Optional(Adt("Mammals".to_string()).into()).into()),
                &Array(Optional(Adt("Dog".to_string()).into()).into())
            ),
            Array(Optional(Adt("Dog".to_string()).into()).into())
        );

        // Map with ADT keys and function values
        assert_eq!(
            reg.greatest_lower_bound(
                &Map(
                    Adt("Dog".to_string()).into(),
                    Closure(
                        Adt("Car".to_string()).into(),
                        Adt("Boat".to_string()).into()
                    )
                    .into()
                ),
                &Map(
                    Adt("Animals".to_string()).into(),
                    Closure(
                        Adt("LandVehicles".to_string()).into(),
                        Adt("WaterVehicles".to_string()).into()
                    )
                    .into()
                )
            ),
            Map(
                Adt("Animals".to_string()).into(), // LUB of keys
                Closure(
                    Adt("LandVehicles".to_string()).into(), // LUB of function parameters
                    Adt("Boat".to_string()).into()          // GLB of function returns
                )
                .into()
            )
        );

        // Optional Stored Tuple of ADTs
        assert_eq!(
            reg.greatest_lower_bound(
                &Optional(
                    Stored(
                        Tuple(vec![
                            Adt("Mammals".to_string()).into(),
                            Adt("LandVehicles".to_string()).into()
                        ])
                        .into()
                    )
                    .into()
                ),
                &Optional(
                    Stored(
                        Tuple(vec![
                            Adt("Dog".to_string()).into(),
                            Adt("Car".to_string()).into()
                        ])
                        .into()
                    )
                    .into()
                )
            ),
            Optional(
                Stored(
                    Tuple(vec![
                        Adt("Dog".to_string()).into(),
                        Adt("Car".to_string()).into()
                    ])
                    .into()
                )
                .into()
            )
        );
    }
}
