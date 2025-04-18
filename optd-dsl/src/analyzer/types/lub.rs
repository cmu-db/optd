use super::registry::TypeRegistry;
use crate::analyzer::{hir::Identifier, types::registry::Type};
use std::collections::HashSet;

impl TypeRegistry {
    /// Finds the least upper bound (LUB) of two types.
    /// The least upper bound is the most specific type that is a supertype of both input types.
    ///
    /// The LUB follows these principles:
    /// 1. Primitive types check for equality.
    /// 2. For container types (Array, Tuple, etc.), it applies covariance rules.
    /// 3. For function (and Map) types, it uses contravariance for parameters and covariance for return types.
    /// 4. For native trait types (Concat, EqHash, Arithmetic), the result is the trait only if both types
    ///    implement it, and at least one of the types is the trait itself.
    /// 5. For ADT types, it finds the closest common supertype in the type hierarchy.
    /// 6. For wrapper types:
    ///    - Optional preserves the wrapper and computes LUB of inner types
    ///    - None and Optional(T) yields Optional(T)
    ///    - None and non-Optional T yields Optional(T)
    ///    - For Stored/Costed: Costed is considered more specific than Stored, and
    ///      either wrapper can be removed when comparing with non-wrapped types
    /// 7. Map types can be viewed as functions from keys to optional values, and
    ///    Arrays as functions from indices to values, with appropriate type conversions.
    /// 8. The ultimate fallback is Type::Universe, the top type of the hierarchy.
    /// 9. Nothing is the bottom type; LUB(Nothing, T) = T.
    ///
    /// # Arguments
    ///
    /// * `type1` - The first type
    /// * `type2` - The second type
    ///
    /// # Returns
    ///
    /// The least upper bound of the two types. Returns `Type::Universe` if no more specific common
    /// supertype exists.
    pub(super) fn least_upper_bound(&mut self, type1: &Type, type2: &Type) -> Type {
        use Type::*;

        let lub = match (type1, type2) {
            // Substitute Unknown types with their current inferred type.
            (unknown @ Unknown(_), other) | (other, unknown @ Unknown(_)) => {
                let bound_unknown = self.resolve_type(unknown);
                self.least_upper_bound(&bound_unknown, other)
            }

            // Nothing is the bottom type - LUB(Nothing, T) = T.
            (Nothing, other) | (other, Nothing) => other.clone(),

            // Primitive types - check for equality.
            (I64, I64)
            | (String, String)
            | (F64, F64)
            | (Bool, Bool)
            | (Unit, Unit)
            | (None, None) => type1.clone(),

            // Array covariance: LUB(Array<T1>, Array<T2>) = Array<LUB(T1, T2)>.
            (Array(elem1), Array(elem2)) => {
                let lub_elem = self.least_upper_bound(elem1, elem2);
                Array(lub_elem.into())
            }

            // Tuple covariance: LUB((T1,T2,...), (U1,U2,...)) = (LUB(T1,U1), LUB(T2,U2), ...).
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let lub_elems = elems1
                    .iter()
                    .zip(elems2.iter())
                    .map(|(e1, e2)| self.least_upper_bound(e1, e2))
                    .collect();
                Tuple(lub_elems)
            }

            // Map with contravariant keys and covariant values.
            (Map(key1, val1), Map(key2, val2)) => {
                let glb_key = self.greatest_lower_bound(key1, key2);
                let lub_val = self.least_upper_bound(val1, val2);
                Map(glb_key.into(), lub_val.into())
            }

            // Optional type handling.
            (Optional(inner1), Optional(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Optional(lub_inner.into())
            }
            (None, Optional(inner)) | (Optional(inner), None) => Optional(inner.clone()),
            (Optional(inner), other) | (other, Optional(inner)) => {
                Optional(self.least_upper_bound(inner, other).into())
            }
            (None, other) | (other, None) if !matches!(other, Optional(_)) => {
                Optional(other.clone().into())
            }

            // Stored type handling.
            (Stored(inner1), Stored(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Stored(lub_inner.into())
            }

            // Costed type handling.
            (Costed(inner1), Costed(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Costed(lub_inner.into())
            }

            // Mixed Stored and Costed - result is Stored (Costed is more specific, Stored is more general).
            (Costed(costed), Stored(stored)) | (Stored(stored), Costed(costed)) => {
                let lub_inner = self.least_upper_bound(costed, stored);
                Stored(lub_inner.into())
            }

            // Unwrap Stored/Costed for LUB with other types.
            (Stored(stored), other) | (other, Stored(stored)) => {
                self.least_upper_bound(stored, other)
            }
            (Costed(costed), other) | (other, Costed(costed)) => {
                self.least_upper_bound(costed, other)
            }

            // Function type handling - contravariant parameters, covariant return types.
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_type = self.greatest_lower_bound(param1, param2);
                let ret_type = self.least_upper_bound(ret1, ret2);
                Closure(param_type.into(), ret_type.into())
            }

            // Map/Function compatibility - a Map can be seen as a function from keys to optional values.
            (Map(key_type, val_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Map(key_type, val_type)) => {
                let param_glb = self.greatest_lower_bound(param_type, key_type);
                let ret_lub = self.least_upper_bound(ret_type, &Optional(val_type.clone()));
                Closure(param_glb.into(), ret_lub.into())
            }

            // Array/Function compatibility - an Array can be seen as a function from indices to values.
            (Array(elem_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Array(elem_type)) => {
                if matches!(&**param_type, I64) {
                    let ret_lub = self.least_upper_bound(ret_type, &Optional(elem_type.clone()));
                    Closure(I64.into(), ret_lub.into())
                } else {
                    Universe
                }
            }

            // ADT types - find their common supertype in the hierarchy.
            (Adt(name1), Adt(name2)) => {
                if let Some(common_ancestor) = self.find_common_supertype(name1, name2) {
                    Adt(common_ancestor)
                } else {
                    Universe
                }
            }

            // Native trait handling.
            (trait_type @ (Concat | EqHash | Arithmetic), other)
            | (other, trait_type @ (Concat | EqHash | Arithmetic))
                if self.is_subtype(other, trait_type) =>
            {
                trait_type.clone()
            }

            // Default case - universe is the ultimate fallback.
            _ => Universe,
        };

        // Verify post-condition.
        debug_assert!(self.is_subtype(type1, &lub));
        debug_assert!(self.is_subtype(type2, &lub));

        lub
    }

    /// Finds a common supertype of two ADT identifiers in the type hierarchy.
    ///
    /// # Arguments
    ///
    /// * `name1` - The first ADT identifier
    /// * `name2` - The second ADT identifier
    ///
    /// # Returns
    ///
    /// Some(identifier) if a common supertype is found, None otherwise.
    fn find_common_supertype(
        &mut self,
        name1: &Identifier,
        name2: &Identifier,
    ) -> Option<Identifier> {
        let supertypes1 = self.get_all_supertypes(name1);
        let supertypes2 = self.get_all_supertypes(name2);

        let common_supertypes: Vec<_> = supertypes1
            .iter()
            .filter(|&st| supertypes2.contains(st))
            .collect();

        if common_supertypes.is_empty() {
            return None;
        }

        let mut most_specific = common_supertypes[0];
        for &supertype in &common_supertypes[1..] {
            if self.is_subtype(
                &Type::Adt(supertype.clone()),
                &Type::Adt(most_specific.clone()),
            ) {
                most_specific = supertype;
            }
        }

        Some(most_specific.clone())
    }

    /// Gets all supertypes of an ADT identifier in the type hierarchy.
    ///
    /// # Arguments
    ///
    /// * `name` - The ADT identifier
    ///
    /// # Returns
    ///
    /// A HashSet containing all supertypes of the given ADT, including itself.
    fn get_all_supertypes(&self, name: &Identifier) -> HashSet<Identifier> {
        let mut result = HashSet::new();
        result.insert(name.clone());

        // Inner recursive function to collect supertypes.
        fn collect_supertypes(
            registry: &TypeRegistry,
            name: &Identifier,
            result: &mut HashSet<Identifier>,
        ) {
            for (parent, children) in &registry.subtypes {
                if children.contains(name) {
                    result.insert(parent.clone());
                    collect_supertypes(registry, parent, result);
                }
            }
        }

        collect_supertypes(self, name, &mut result);
        result
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        analyzer::types::registry::type_registry_tests::{create_product_adt, create_sum_adt},
        parser::ast::Type as AstType,
    };

    /// Helper function to set up a comprehensive type hierarchy for testing
    pub fn setup_type_hierarchy() -> TypeRegistry {
        let mut registry = TypeRegistry::default();

        // Animal hierarchy
        let animal = create_product_adt("Animal", vec![("name", AstType::String)]);

        let mammal = create_product_adt(
            "Mammal",
            vec![("name", AstType::String), ("warm_blooded", AstType::Bool)],
        );

        let bird = create_product_adt(
            "Bird",
            vec![("name", AstType::String), ("can_fly", AstType::Bool)],
        );

        let dog = create_product_adt(
            "Dog",
            vec![
                ("name", AstType::String),
                ("warm_blooded", AstType::Bool),
                ("breed", AstType::String),
            ],
        );

        let cat = create_product_adt(
            "Cat",
            vec![
                ("name", AstType::String),
                ("warm_blooded", AstType::Bool),
                ("lives", AstType::Int64),
            ],
        );

        let eagle = create_product_adt(
            "Eagle",
            vec![
                ("name", AstType::String),
                ("can_fly", AstType::Bool),
                ("wingspan", AstType::Float64),
            ],
        );

        let penguin = create_product_adt(
            "Penguin",
            vec![
                ("name", AstType::String),
                ("can_fly", AstType::Bool),
                ("swims", AstType::Bool),
            ],
        );

        // Vehicle hierarchy
        let vehicle = create_product_adt(
            "Vehicle",
            vec![("id", AstType::String), ("wheels", AstType::Int64)],
        );

        let land_vehicle = create_product_adt(
            "LandVehicle",
            vec![
                ("id", AstType::String),
                ("wheels", AstType::Int64),
                ("terrain", AstType::String),
            ],
        );

        let water_vehicle = create_product_adt(
            "WaterVehicle",
            vec![
                ("id", AstType::String),
                ("wheels", AstType::Int64),
                ("displacement", AstType::Float64),
            ],
        );

        let car = create_product_adt(
            "Car",
            vec![
                ("id", AstType::String),
                ("wheels", AstType::Int64),
                ("terrain", AstType::String),
                ("doors", AstType::Int64),
            ],
        );

        let bicycle = create_product_adt(
            "Bicycle",
            vec![
                ("id", AstType::String),
                ("wheels", AstType::Int64),
                ("terrain", AstType::String),
                ("pedals", AstType::Bool),
            ],
        );

        let boat = create_product_adt(
            "Boat",
            vec![
                ("id", AstType::String),
                ("wheels", AstType::Int64),
                ("displacement", AstType::Float64),
                ("sails", AstType::Bool),
            ],
        );

        // Register hierarchies
        let mammals_enum = create_sum_adt("Mammals", vec![mammal.clone(), dog, cat]);
        let birds_enum = create_sum_adt("Birds", vec![bird.clone(), eagle, penguin]);
        let animals_enum = create_sum_adt("Animals", vec![animal, mammals_enum, birds_enum]);

        let land_vehicles_enum = create_sum_adt("LandVehicles", vec![land_vehicle, car, bicycle]);
        let water_vehicles_enum = create_sum_adt("WaterVehicles", vec![water_vehicle, boat]);
        let vehicles_enum = create_sum_adt(
            "Vehicles",
            vec![vehicle, land_vehicles_enum, water_vehicles_enum],
        );

        registry.register_adt(&animals_enum).unwrap();
        registry.register_adt(&vehicles_enum).unwrap();

        registry
    }

    #[test]
    fn test_simple_lub() {
        let mut registry = setup_type_hierarchy();

        // Identical primitive types
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::I64),
            Type::I64
        );
        assert_eq!(
            registry.least_upper_bound(&Type::Bool, &Type::Bool),
            Type::Bool
        );
        assert_eq!(
            registry.least_upper_bound(&Type::String, &Type::String),
            Type::String
        );

        // Different primitive types
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::Bool),
            Type::Universe
        );
        assert_eq!(
            registry.least_upper_bound(&Type::String, &Type::F64),
            Type::Universe
        );
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::F64),
            Type::Universe
        );

        // ADTs
        assert_eq!(
            registry
                .least_upper_bound(&Type::Adt("Dog".to_string()), &Type::Adt("Dog".to_string())),
            Type::Adt("Dog".to_string())
        );

        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Dog".to_string()),
                &Type::Adt("Mammal".to_string())
            ),
            Type::Adt("Mammals".to_string())
        );

        assert_eq!(
            registry
                .least_upper_bound(&Type::Adt("Dog".to_string()), &Type::Adt("Cat".to_string())),
            Type::Adt("Mammals".to_string())
        );

        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Dog".to_string()),
                &Type::Adt("Eagle".to_string())
            ),
            Type::Adt("Animals".to_string())
        );

        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Mammals".to_string()),
                &Type::Adt("Birds".to_string())
            ),
            Type::Adt("Animals".to_string())
        );

        // ADT and unrelated type
        assert_eq!(
            registry
                .least_upper_bound(&Type::Adt("Dog".to_string()), &Type::Adt("Car".to_string())),
            Type::Universe
        );

        // Nothing with other types
        assert_eq!(
            registry.least_upper_bound(&Type::Nothing, &Type::Adt("Dog".to_string())),
            Type::Adt("Dog".to_string())
        );

        assert_eq!(
            registry.least_upper_bound(&Type::Adt("Car".to_string()), &Type::Nothing),
            Type::Adt("Car".to_string())
        );
    }

    #[test]
    fn test_array_lub() {
        let mut registry = setup_type_hierarchy();

        // Array of same ADT type
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Array(Box::new(Type::Adt("Dog".to_string())))
        );

        // Array of related ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Cat".to_string())))
            ),
            Type::Array(Box::new(Type::Adt("Mammals".to_string())))
        );

        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Eagle".to_string())))
            ),
            Type::Array(Box::new(Type::Adt("Animals".to_string())))
        );

        // Array of unrelated ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Array(Box::new(Type::Adt("Car".to_string())))
            ),
            Type::Array(Box::new(Type::Universe))
        );

        // Nested arrays with ADTs
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Array(Box::new(Type::Adt(
                    "Dog".to_string()
                ))))),
                &Type::Array(Box::new(Type::Array(Box::new(Type::Adt(
                    "Cat".to_string()
                )))))
            ),
            Type::Array(Box::new(Type::Array(Box::new(Type::Adt(
                "Mammals".to_string()
            )))))
        );
    }

    #[test]
    fn test_tuple_lub() {
        let mut registry = setup_type_hierarchy();

        // Tuples with same ADT types
        assert_eq!(
            registry.least_upper_bound(
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

        // Tuples with related ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ]),
                &Type::Tuple(vec![
                    Type::Adt("Cat".to_string()),
                    Type::Adt("Bicycle".to_string())
                ])
            ),
            Type::Tuple(vec![
                Type::Adt("Mammals".to_string()),
                Type::Adt("LandVehicles".to_string())
            ])
        );

        // Tuples with mixed related/unrelated types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ]),
                &Type::Tuple(vec![
                    Type::Adt("Eagle".to_string()),
                    Type::Adt("Boat".to_string())
                ])
            ),
            Type::Tuple(vec![
                Type::Adt("Animals".to_string()),
                Type::Adt("Vehicles".to_string())
            ])
        );

        // Different length tuples should result in Universe
        assert_eq!(
            registry.least_upper_bound(
                &Type::Tuple(vec![Type::Adt("Dog".to_string())]),
                &Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ])
            ),
            Type::Universe
        );
    }

    #[test]
    fn test_map_lub() {
        let mut registry = setup_type_hierarchy();

        // Maps with identical ADT key and value types
        assert_eq!(
            registry.least_upper_bound(
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

        // Maps with related ADT value types (covariant)
        assert_eq!(
            registry.least_upper_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Bicycle".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("LandVehicles".to_string()))
            )
        );

        // Maps with related ADT key types (contravariant)
        assert_eq!(
            registry.least_upper_bound(
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
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Maps with both key and value types related
        assert_eq!(
            registry.least_upper_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Adt("Bicycle".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("LandVehicles".to_string()))
            )
        );

        // Maps with unrelated value types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Map(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Boat".to_string()))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Vehicles".to_string()))
            )
        );
    }

    #[test]
    fn test_function_lub() {
        let mut registry = setup_type_hierarchy();

        // Functions with identical ADT parameter and return types
        assert_eq!(
            registry.least_upper_bound(
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

        // Functions with related ADT parameter types (contravariance)
        assert_eq!(
            registry.least_upper_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Cat".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Nothing),
                Box::new(Type::Adt("Car".to_string()))
            )
        );

        // Functions with related ADT return types (covariance)
        assert_eq!(
            registry.least_upper_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Bicycle".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("LandVehicles".to_string()))
            )
        );

        // Functions with both parameter and return types related
        assert_eq!(
            registry.least_upper_bound(
                &Type::Closure(
                    Box::new(Type::Adt("Dog".to_string())),
                    Box::new(Type::Adt("Car".to_string()))
                ),
                &Type::Closure(
                    Box::new(Type::Adt("Mammals".to_string())),
                    Box::new(Type::Adt("Bicycle".to_string()))
                )
            ),
            Type::Closure(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("LandVehicles".to_string()))
            )
        );
    }

    #[test]
    fn test_optional_lub() {
        let mut registry = setup_type_hierarchy();

        // Optional of same ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Optional(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Optional(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        );

        // Optional of related ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Optional(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Optional(Box::new(Type::Adt("Cat".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Mammals".to_string())))
        );

        // Optional of distantly related ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Optional(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Optional(Box::new(Type::Adt("Eagle".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Animals".to_string())))
        );

        // None and Optional ADT
        assert_eq!(
            registry.least_upper_bound(
                &Type::None,
                &Type::Optional(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        );

        // None and ADT type
        assert_eq!(
            registry.least_upper_bound(&Type::None, &Type::Adt("Dog".to_string())),
            Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        );

        // ADT and Optional related ADT
        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Dog".to_string()),
                &Type::Optional(Box::new(Type::Adt("Cat".to_string())))
            ),
            Type::Optional(Box::new(Type::Adt("Mammals".to_string())))
        );
    }

    #[test]
    fn test_stored_costed_lub() {
        let mut registry = setup_type_hierarchy();

        // Stored of same ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Stored(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Stored(Box::new(Type::Adt("Dog".to_string())))
        );

        // Stored of related ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Stored(Box::new(Type::Adt("Cat".to_string())))
            ),
            Type::Stored(Box::new(Type::Adt("Mammals".to_string())))
        );

        // Costed of same ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Costed(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Costed(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Costed(Box::new(Type::Adt("Dog".to_string())))
        );

        // Mixed Stored and Costed with same ADT type
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Costed(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Stored(Box::new(Type::Adt("Dog".to_string())))
        );

        // Mixed Stored and Costed with related ADT types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Costed(Box::new(Type::Adt("Cat".to_string())))
            ),
            Type::Stored(Box::new(Type::Adt("Mammals".to_string())))
        );

        // Stored/Costed with regular ADT type
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Adt("Cat".to_string())
            ),
            Type::Adt("Mammals".to_string())
        );
    }

    #[test]
    fn test_native_traits_with_adts() {
        let mut registry = setup_type_hierarchy();

        // EqHash with ADT types
        assert_eq!(
            registry.least_upper_bound(&Type::EqHash, &Type::Adt("Dog".to_string())),
            Type::EqHash
        );

        // Concat with Array of ADTs
        assert_eq!(
            registry.least_upper_bound(
                &Type::Concat,
                &Type::Array(Box::new(Type::Adt("Dog".to_string())))
            ),
            Type::Concat
        );

        // Arithmetic with numeric type
        assert_eq!(
            registry.least_upper_bound(&Type::Arithmetic, &Type::I64),
            Type::Arithmetic
        );
    }

    #[test]
    fn test_complex_nested_types_lub() {
        let mut registry = setup_type_hierarchy();

        // Array of Optional ADTs
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Optional(Box::new(Type::Adt(
                    "Dog".to_string()
                ))))),
                &Type::Array(Box::new(Type::Optional(Box::new(Type::Adt(
                    "Cat".to_string()
                )))))
            ),
            Type::Array(Box::new(Type::Optional(Box::new(Type::Adt(
                "Mammals".to_string()
            )))))
        );

        // Map with ADT keys and function values
        assert_eq!(
            registry.least_upper_bound(
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
                        Box::new(Type::Adt("Bicycle".to_string())),
                        Box::new(Type::Adt("Boat".to_string()))
                    ))
                )
            ),
            Type::Map(
                Box::new(Type::Adt("Dog".to_string())), // contravariance on key type
                Box::new(Type::Closure(
                    Box::new(Type::Nothing), // contravariance on function parameter
                    Box::new(Type::Adt("Boat".to_string()))  // covariance on function return
                ))
            )
        );

        // Optional Stored Tuple of ADTs
        assert_eq!(
            registry.least_upper_bound(
                &Type::Optional(Box::new(Type::Stored(Box::new(Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Adt("Car".to_string())
                ]))))),
                &Type::Optional(Box::new(Type::Stored(Box::new(Type::Tuple(vec![
                    Type::Adt("Cat".to_string()),
                    Type::Adt("Bicycle".to_string())
                ])))))
            ),
            Type::Optional(Box::new(Type::Stored(Box::new(Type::Tuple(vec![
                Type::Adt("Mammals".to_string()),
                Type::Adt("LandVehicles".to_string())
            ])))))
        );
    }

    #[test]
    fn test_map_function_compatibility_lub() {
        let mut registry = setup_type_hierarchy();

        // Map and Function compatibility
        assert_eq!(
            registry.least_upper_bound(
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
            Type::Closure(
                Box::new(Type::Adt("Dog".to_string())), // GLB of Dog and Mammals is Dog (contravariance)
                Box::new(Type::Optional(Box::new(Type::Adt(
                    "LandVehicles".to_string()
                ))))  // LUB of Car? and LandVehicles?
            )
        );

        // Array and Function compatibility
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
                &Type::Closure(
                    Box::new(Type::I64),
                    Box::new(Type::Optional(Box::new(Type::Adt("Mammals".to_string()))))
                )
            ),
            Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::Optional(Box::new(Type::Adt("Mammals".to_string()))))
            )
        );
    }
}
