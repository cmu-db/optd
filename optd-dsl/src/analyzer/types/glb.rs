use super::registry::TypeRegistry;
use crate::analyzer::types::registry::Type;

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
    pub(super) fn greatest_lower_bound(&mut self, type1: &Type, type2: &Type) -> Type {
        use Type::*;

        let glb = match (type1, type2) {
            // Substitute Unknown types with their current inferred type.
            (Unknown(id), _) | (_, Unknown(id)) => {
                let type1 = self.resolved_unknown.get(id).cloned().unwrap();
                self.least_upper_bound(&type1, type2)
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
                    .map(|(e1, e2)| self.greatest_lower_bound(e1, e2))
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
            (adt1 @ Adt(_), adt2 @ Adt(_)) => {
                if self.is_subtype(adt1, adt2) {
                    adt1.clone()
                } else if self.is_subtype(adt2, adt1) {
                    adt2.clone()
                } else {
                    Nothing
                }
            }

            // Native trait handling.
            (trait_type @ (Concat | EqHash | Arithmetic), other)
            | (other, trait_type @ (Concat | EqHash | Arithmetic))
                if self.is_subtype(other, trait_type) =>
            {
                other.clone()
            }

            // Default case - return Nothing if no common subtype exists.
            _ => Nothing,
        };

        // Verify post-condition.
        assert!(self.is_subtype(&glb, type1));
        assert!(self.is_subtype(&glb, type2));

        glb
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::types::lub::tests::setup_type_hierarchy;

    #[test]
    fn test_greatest_lower_bound_simple() {
        let mut registry = setup_type_hierarchy();

        // Identical ADT types
        assert_eq!(
            registry
                .greatest_lower_bound(&Type::Adt("Dog".to_string()), &Type::Adt("Dog".to_string())),
            Type::Adt("Dog".to_string())
        );

        // Subtype/supertype relationship
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Adt("Dog".to_string()),
                &Type::Adt("Mammals".to_string())
            ),
            Type::Adt("Dog".to_string())
        );

        // Related types (siblings)
        assert_eq!(
            registry
                .greatest_lower_bound(&Type::Adt("Dog".to_string()), &Type::Adt("Cat".to_string())),
            Type::Nothing
        );

        // Unrelated types
        assert_eq!(
            registry
                .greatest_lower_bound(&Type::Adt("Dog".to_string()), &Type::Adt("Car".to_string())),
            Type::Nothing
        );

        // Universe with ADT type
        assert_eq!(
            registry.greatest_lower_bound(&Type::Universe, &Type::Adt("Dog".to_string())),
            Type::Adt("Dog".to_string())
        );

        // Nothing with ADT type
        assert_eq!(
            registry.greatest_lower_bound(&Type::Nothing, &Type::Adt("Dog".to_string())),
            Type::Nothing
        );
    }

    #[test]
    fn test_greatest_lower_bound_array() {
        let mut registry = setup_type_hierarchy();

        // Arrays of same ADT types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Array(Box::new(Type::Adt("Dog".to_string())))
        );

        // Arrays of subtype/supertype relationship
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Mammals".to_string())))
            ),
            Type::Array(Box::new(Type::Adt("Dog".to_string())))
        );

        // Arrays of related types (siblings)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Cat".to_string())))
            ),
            Type::Array(Box::new(Type::Nothing))
        );
    }

    #[test]
    fn test_greatest_lower_bound_tuple() {
        let mut registry = setup_type_hierarchy();

        // Tuples of same ADT types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ]),
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ])
            ),
            Type::Tuple(vec![
                Type::Adt("Dog".to_string()),
                Type::Adt("Car".to_string())
            ])
        );

        // Tuples with subtype/supertype relationship
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ]),
                &Type::Tuple(vec![
                    Type::Adt("Mammals".to_string()),
                    Type::Adt("LandVehicles".to_string())
                ])
            ),
            Type::Tuple(vec![
                Type::Adt("Dog".to_string()),
                Type::Adt("Car".to_string())
            ])
        );

        // Tuples with mixed compatibility
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ]),
                &Type::Tuple(vec![
                    Type::Adt("Cat".to_string()),
                    Type::Adt("LandVehicles".to_string())
                ])
            ),
            Type::Tuple(vec![Type::Nothing, Type::Adt("Car".to_string())])
        );

        // Different length tuples
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Tuple(vec![Type::Adt("Dog".to_string())]),
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ])
            ),
            Type::Nothing
        );
    }

    #[test]
    fn test_greatest_lower_bound_map() {
        let mut registry = setup_type_hierarchy();

        // Maps with same key and value types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Maps with covariant keys (opposite of LUB due to contravariance)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Mammals".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Maps with contravariant values (opposite of LUB due to covariance)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("LandVehicles".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Maps with mixed key and value type relationships
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Adt("LandVehicles".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Mammals".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );
    }

    #[test]
    fn test_greatest_lower_bound_function() {
        let mut registry = setup_type_hierarchy();

        // Functions with same parameter and return types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Functions with covariant parameters (opposite of LUB due to contravariance)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Adt("Mammals".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Functions with contravariant return types (opposite of LUB due to covariance)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("LandVehicles".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Functions with mixed parameter and return type relationships
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Adt("LandVehicles".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Adt("Mammals".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );
    }

    #[test]
    fn test_greatest_lower_bound_optional() {
        let mut registry = setup_type_hierarchy();

        // Optional of same types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Optional(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        );

        // Optional of subtype/supertype
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Optional(Box::new(Type::Adt("Mammals".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        );

        // Optional of unrelated types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Optional(Box::new(Type::Adt("Car".to_string())))
            ),
            Type::Optional(Box::new(Type::Nothing))
        );

        // None and Optional
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::None,
                &Type::Optional(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::None
        );

        // None and None
        assert_eq!(
            registry.greatest_lower_bound(&Type::None, &Type::None),
            Type::None
        );

        // Optional and base type
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::Adt("Mammals".to_string()))),
                &Type::Adt("Dog".to_string())
            ),
            Type::Adt("Dog".to_string())
        );
    }

    #[test]
    fn test_greatest_lower_bound_stored_costed() {
        let mut registry = setup_type_hierarchy();

        // Stored of same types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Stored(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Stored(Box::new(Type::Adt("Dog".to_string())))
        );

        // Stored of subtype/supertype
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Stored(Box::new(Type::Adt("Mammals".to_string())))
            ),
            Type::Stored(Box::new(Type::Adt("Dog".to_string())))
        );

        // Costed of same types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Costed(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Costed(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Costed(Box::new(Type::Adt("Dog".to_string())))
        );

        // Mixed Stored and Costed (Costed is more specific than Stored)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Costed(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Costed(Box::new(Type::Adt("Dog".to_string())))
        );

        // Mixed with subtype/supertype relationship
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Stored(Box::new(Type::Adt("Mammals".to_string()))),
                &Type::Costed(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Costed(Box::new(Type::Adt("Dog".to_string())))
        );
    }

    #[test]
    fn test_greatest_lower_bound_collection_function() {
        let mut registry = setup_type_hierarchy();

        // Map and Function compatibility
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Optional(Box::new(Type::Adt(
                        "LandVehicles".to_string()
                    ))))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Mammals".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Array and Function compatibility
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Closure(
                    Box::new(Type::I64),
                    Box::new(Type::Optional(Box::new(Type::Adt("Mammals".to_string()))))
                )
            ),
            Type::Array(Box::new(Type::Adt("Dog".to_string())))
        );
    }

    #[test]
    fn test_greatest_lower_bound_complex_nested_types() {
        let mut registry = setup_type_hierarchy();

        // Array of Optional ADTs
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::Optional(Box::new(Type::Adt(
                    "Mammals".to_string()
                ))))),
                &Type::Array(Box::new(Type::Optional(Box::new(Type::Adt(
                    "Dog".to_string()
                )))))
            ),
            Type::Array(Box::new(Type::Optional(Box::new(Type::Adt(
                "Dog".to_string()
            )))))
        );

        // Map with ADT keys and function values
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Closure(
                        Box::new(Type::Adt("Car".to_string())),
                        Box::new(Type::Adt("Boat".to_string()))
                    ))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Animals".to_string())),
                    Box::new(Type::Closure(
                        Box::new(Type::Adt("LandVehicles".to_string())),
                        Box::new(Type::Adt("WaterVehicles".to_string()))
                    ))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Animals".to_string())), // LUB of keys
                Box::new(Type::Closure(
                    Box::new(Type::Adt("LandVehicles".to_string())), // LUB of function parameters
                    Box::new(Type::Adt("Boat".to_string()))          // GLB of function returns
                ))
            )
        );

        // Optional Stored Tuple of ADTs
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::Stored(Box::new(Type::Tuple(vec![
                    Type::Adt("Mammals".to_string()),
                    Type::Adt("LandVehicles".to_string())
                ]))))),
                &Type::Optional(Box::new(Type::Stored(Box::new(Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ])))))
            ),
            Type::Optional(Box::new(Type::Stored(Box::new(Type::Tuple(vec![
                Type::Adt("Dog".to_string()),
                Type::Adt("Car".to_string())
            ])))))
        );
    }
}
