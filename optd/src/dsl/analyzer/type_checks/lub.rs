use super::registry::{Type, TypeRegistry};
use crate::dsl::analyzer::{hir::Identifier, type_checks::registry::TypeKind};
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
    /// 8. Nothing is the bottom type, Universe it the top type; LUB(Nothing, T) = T and LUB(Universe, T) = Universe.
    /// 9. If no meaningful upper bound exists, Universe is returned.
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
    /// The least upper bound of the two types, or Universe if no common supertype exists.
    pub(super) fn least_upper_bound(
        &mut self,
        type1: &Type,
        type2: &Type,
        has_changed: &mut bool,
    ) -> Type {
        use TypeKind::*;

        let lub_kind = match (&*type1.value, &*type2.value) {
            // Substitute Unknown types with their current inferred type.
            (UnknownAsc(_) | UnknownDesc(_), _) => {
                let bound_unknown = self.resolve_type(type1);
                return self.least_upper_bound(&bound_unknown, type2, has_changed);
            }
            (_, UnknownAsc(_) | UnknownDesc(_)) => {
                let bound_unknown = self.resolve_type(type2);
                return self.least_upper_bound(type1, &bound_unknown, has_changed);
            }

            // Universe is the top type - LUB(Universe, T) = Universe.
            (Universe, _) | (_, Universe) => Universe,

            // Nothing is the bottom type - LUB(Nothing, T) = T.
            (Nothing, other) => other.clone(),
            (other, Nothing) => other.clone(),

            // Primitive types - check for equality.
            (I64, I64)
            | (String, String)
            | (F64, F64)
            | (Bool, Bool)
            | (Unit, Unit)
            | (None, None) => *type1.value.clone(),

            // Array covariance: LUB(Array<T1>, Array<T2>) = Array<LUB(T1, T2)>.
            (Array(elem1), Array(elem2)) => {
                let lub_elem = self.least_upper_bound(elem1, elem2, has_changed);
                Array(lub_elem)
            }

            // Tuple covariance: LUB((T1,T2,...), (U1,U2,...)) = (LUB(T1,U1), LUB(T2,U2), ...).
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let mut lub_elems = Vec::with_capacity(elems1.len());
                for (e1, e2) in elems1.iter().zip(elems2.iter()) {
                    lub_elems.push(self.least_upper_bound(e1, e2, has_changed));
                }
                Tuple(lub_elems)
            }

            // Map with contravariant keys and covariant values.
            (Map(key1, val1), Map(key2, val2)) => {
                let glb_key = self.greatest_lower_bound(key1, key2, has_changed);
                let lub_val = self.least_upper_bound(val1, val2, has_changed);
                Map(glb_key, lub_val)
            }

            // Optional type handling.
            (Optional(inner1), Optional(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2, has_changed);
                Optional(lub_inner)
            }
            (None, Optional(inner)) | (Optional(inner), None) => Optional(inner.clone()),
            (Optional(inner), _) => Optional(self.least_upper_bound(inner, type2, has_changed)),
            (_, Optional(inner)) => Optional(self.least_upper_bound(type1, inner, has_changed)),
            (None, _) => Optional(type2.clone()),
            (_, None) => Optional(type1.clone()),

            // Stored type handling.
            (Stored(inner1), Stored(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2, has_changed);
                Stored(lub_inner)
            }

            // Costed type handling.
            (Costed(inner1), Costed(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2, has_changed);
                Costed(lub_inner)
            }

            // Mixed Stored and Costed - result is Stored (Costed is more specific, Stored is more general).
            (Costed(costed), Stored(stored)) | (Stored(stored), Costed(costed)) => {
                let lub_inner = self.least_upper_bound(costed, stored, has_changed);
                Stored(lub_inner)
            }

            // Unwrap Stored/Costed for LUB with other types.
            (Stored(stored), _) => {
                return self.least_upper_bound(stored, type2, has_changed);
            }
            (_, Stored(stored)) => {
                return self.least_upper_bound(type1, stored, has_changed);
            }
            (Costed(costed), _) => {
                return self.least_upper_bound(costed, type2, has_changed);
            }
            (_, Costed(costed)) => {
                return self.least_upper_bound(type1, costed, has_changed);
            }

            // Function type handling - contravariant parameters, covariant return types.
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_type = self.greatest_lower_bound(param1, param2, has_changed);
                let ret_type = self.least_upper_bound(ret1, ret2, has_changed);
                Closure(param_type, ret_type)
            }

            // Map/Function compatibility - a Map can be seen as a function from keys to optional values.
            (Map(key_type, val_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Map(key_type, val_type)) => {
                let param_glb = self.greatest_lower_bound(param_type, key_type, has_changed);
                let ret_lub = self.least_upper_bound(
                    ret_type,
                    &Optional(val_type.clone()).into(),
                    has_changed,
                );
                Closure(param_glb, ret_lub)
            }

            // Array/Function compatibility - an Array can be seen as a function from indices to values.
            (Array(elem_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Array(elem_type))
                if matches!(&*param_type.value, I64) =>
            {
                let ret_lub = self.least_upper_bound(
                    ret_type,
                    &Optional(elem_type.clone()).into(),
                    has_changed,
                );
                Closure(I64.into(), ret_lub)
            }

            // ADT types - find their common supertype in the hierarchy.
            (Adt(name1), Adt(name2)) => {
                if let Some(common_ancestor) = self.find_common_supertype(name1, name2) {
                    Adt(common_ancestor)
                } else {
                    // Return Universe if no common supertype
                    Universe
                }
            }

            // Native trait handling.
            (trait_type @ (Concat | EqHash | Arithmetic), _)
                if self.is_subtype_infer(type2, type1, has_changed) =>
            {
                trait_type.clone()
            }
            (_, trait_type @ (Concat | EqHash | Arithmetic))
                if self.is_subtype_infer(type1, type2, has_changed) =>
            {
                trait_type.clone()
            }

            // Default case - incompatible types.
            _ => Universe,
        };

        let result = lub_kind.into();

        // Verify post-condition in debug mode only
        debug_assert!(
            self.is_subtype(type1, &result),
            "LUB post-condition failed: {:?} is not a subtype of {:?}",
            type1,
            result
        );
        debug_assert!(
            self.is_subtype(type2, &result),
            "LUB post-condition failed: {:?} is not a subtype of {:?}",
            type2,
            result
        );

        result
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
            if self.inherits_adt(supertype, most_specific) {
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
    use crate::dsl::{
        analyzer::type_checks::registry::type_registry_tests::{create_product_adt, create_sum_adt},
        parser::ast::Type as AstType,
    };
    use TypeKind::*;

    /// Helper function to simplify LUB assertions
    pub fn assert_lub_eq(reg: &mut TypeRegistry, t1: &Type, t2: &Type, expected: TypeKind) {
        let mut has_changed = false;
        let result = reg.least_upper_bound(t1, t2, &mut has_changed);
        assert_eq!(result, expected.into());
    }

    /// Helper function to verify LUB returns Universe
    pub fn assert_lub_universe(reg: &mut TypeRegistry, t1: &Type, t2: &Type) {
        let mut has_changed = false;
        let result = reg.least_upper_bound(t1, t2, &mut has_changed);
        assert_eq!(*result.value, Universe);
    }

    /// Helper function to set up a comprehensive type hierarchy for testing
    pub fn setup_type_hierarchy() -> TypeRegistry {
        let mut reg = TypeRegistry::default();

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

        reg.register_adt(&animals_enum).unwrap();
        reg.register_adt(&vehicles_enum).unwrap();

        reg
    }

    #[test]
    fn test_simple_lub() {
        let mut reg = setup_type_hierarchy();

        // Identical primitive types
        assert_lub_eq(&mut reg, &I64.into(), &I64.into(), I64);
        assert_lub_eq(&mut reg, &Bool.into(), &Bool.into(), Bool);
        assert_lub_eq(&mut reg, &String.into(), &String.into(), String);

        // Different primitive types
        assert_lub_universe(&mut reg, &I64.into(), &Bool.into());
        assert_lub_universe(&mut reg, &String.into(), &F64.into());
        assert_lub_universe(&mut reg, &I64.into(), &F64.into());

        // ADTs
        assert_lub_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Dog".to_string()).into(),
            Adt("Dog".to_string()),
        );
        assert_lub_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Mammal".to_string()).into(),
            Adt("Mammals".to_string()),
        );
        assert_lub_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Cat".to_string()).into(),
            Adt("Mammals".to_string()),
        );
        assert_lub_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Eagle".to_string()).into(),
            Adt("Animals".to_string()),
        );
        assert_lub_eq(
            &mut reg,
            &Adt("Mammals".to_string()).into(),
            &Adt("Birds".to_string()).into(),
            Adt("Animals".to_string()),
        );

        // ADT and unrelated type
        assert_lub_universe(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Adt("Car".to_string()).into(),
        );

        // Nothing with other types
        assert_lub_eq(
            &mut reg,
            &Nothing.into(),
            &Adt("Dog".to_string()).into(),
            Adt("Dog".to_string()),
        );
        assert_lub_eq(
            &mut reg,
            &Adt("Car".to_string()).into(),
            &Nothing.into(),
            Adt("Car".to_string()),
        );
    }

    #[test]
    fn test_array_lub() {
        let mut reg = setup_type_hierarchy();

        // Array of same ADT type
        assert_lub_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Dog".to_string()).into()).into(),
            Array(Adt("Dog".to_string()).into()),
        );

        // Array of related ADT types
        assert_lub_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Cat".to_string()).into()).into(),
            Array(Adt("Mammals".to_string()).into()),
        );

        assert_lub_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Eagle".to_string()).into()).into(),
            Array(Adt("Animals".to_string()).into()),
        );

        // Array of unrelated ADT types
        assert_lub_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Array(Adt("Car".to_string()).into()).into(),
            Array(Universe.into()),
        );

        // Nested arrays with ADTs
        assert_lub_eq(
            &mut reg,
            &Array(Array(Adt("Dog".to_string()).into()).into()).into(),
            &Array(Array(Adt("Cat".to_string()).into()).into()).into(),
            Array(Array(Adt("Mammals".to_string()).into()).into()),
        );
    }

    #[test]
    fn test_tuple_lub() {
        let mut reg = setup_type_hierarchy();

        // Tuples with same ADT types
        assert_lub_eq(
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

        // Tuples with related ADT types
        assert_lub_eq(
            &mut reg,
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
            &Tuple(vec![
                Adt("Cat".to_string()).into(),
                Adt("Bicycle".to_string()).into(),
            ])
            .into(),
            Tuple(vec![
                Adt("Mammals".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ]),
        );

        // Tuples with mixed related/unrelated types
        assert_lub_eq(
            &mut reg,
            &Tuple(vec![
                Adt("Dog".to_string()).into(),
                Adt("Car".to_string()).into(),
            ])
            .into(),
            &Tuple(vec![
                Adt("Eagle".to_string()).into(),
                Adt("Boat".to_string()).into(),
            ])
            .into(),
            Tuple(vec![
                Adt("Animals".to_string()).into(),
                Adt("Vehicles".to_string()).into(),
            ]),
        );

        // Different length tuples
        assert_lub_universe(
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
    fn test_map_lub() {
        let mut reg = setup_type_hierarchy();

        // Maps with identical ADT key and value types
        assert_lub_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Maps with related ADT value types (covariant)
        assert_lub_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Dog".to_string()).into(),
                Adt("Bicycle".to_string()).into(),
            )
            .into(),
            Map(
                Adt("Dog".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ),
        );

        // Maps with related ADT key types (contravariant)
        assert_lub_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Mammals".to_string()).into(),
                Adt("Car".to_string()).into(),
            )
            .into(),
            Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Maps with both key and value types related
        assert_lub_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Mammals".to_string()).into(),
                Adt("Bicycle".to_string()).into(),
            )
            .into(),
            Map(
                Adt("Dog".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ),
        );

        // Maps with unrelated value types
        assert_lub_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Map(
                Adt("Dog".to_string()).into(),
                Adt("Boat".to_string()).into(),
            )
            .into(),
            Map(
                Adt("Dog".to_string()).into(),
                Adt("Vehicles".to_string()).into(),
            ),
        );
    }

    #[test]
    fn test_function_lub() {
        let mut reg = setup_type_hierarchy();

        // Functions with identical ADT parameter and return types
        assert_lub_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()),
        );

        // Functions with related ADT parameter types (contravariance)
        assert_lub_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(Adt("Cat".to_string()).into(), Adt("Car".to_string()).into()).into(),
            Closure(Nothing.into(), Adt("Car".to_string()).into()),
        );

        // Functions with related ADT return types (covariance)
        assert_lub_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Dog".to_string()).into(),
                Adt("Bicycle".to_string()).into(),
            )
            .into(),
            Closure(
                Adt("Dog".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ),
        );

        // Functions with both parameter and return types related
        assert_lub_eq(
            &mut reg,
            &Closure(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Mammals".to_string()).into(),
                Adt("Bicycle".to_string()).into(),
            )
            .into(),
            Closure(
                Adt("Dog".to_string()).into(),
                Adt("LandVehicles".to_string()).into(),
            ),
        );
    }

    #[test]
    fn test_optional_lub() {
        let mut reg = setup_type_hierarchy();

        // Optional of same ADT types
        assert_lub_eq(
            &mut reg,
            &Optional(Adt("Dog".to_string()).into()).into(),
            &Optional(Adt("Dog".to_string()).into()).into(),
            Optional(Adt("Dog".to_string()).into()),
        );

        // Optional of related ADT types
        assert_lub_eq(
            &mut reg,
            &Optional(Adt("Dog".to_string()).into()).into(),
            &Optional(Adt("Cat".to_string()).into()).into(),
            Optional(Adt("Mammals".to_string()).into()),
        );

        // Optional of distantly related ADT types
        assert_lub_eq(
            &mut reg,
            &Optional(Adt("Dog".to_string()).into()).into(),
            &Optional(Adt("Eagle".to_string()).into()).into(),
            Optional(Adt("Animals".to_string()).into()),
        );

        // None and Optional ADT
        assert_lub_eq(
            &mut reg,
            &None.into(),
            &Optional(Adt("Dog".to_string()).into()).into(),
            Optional(Adt("Dog".to_string()).into()),
        );

        // None and ADT type
        assert_lub_eq(
            &mut reg,
            &None.into(),
            &Adt("Dog".to_string()).into(),
            Optional(Adt("Dog".to_string()).into()),
        );

        // ADT and Optional related ADT
        assert_lub_eq(
            &mut reg,
            &Adt("Dog".to_string()).into(),
            &Optional(Adt("Cat".to_string()).into()).into(),
            Optional(Adt("Mammals".to_string()).into()),
        );
    }

    #[test]
    fn test_stored_costed_lub() {
        let mut reg = setup_type_hierarchy();

        // Stored of same ADT types
        assert_lub_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Stored(Adt("Dog".to_string()).into()).into(),
            Stored(Adt("Dog".to_string()).into()),
        );

        // Stored of related ADT types
        assert_lub_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Stored(Adt("Cat".to_string()).into()).into(),
            Stored(Adt("Mammals".to_string()).into()),
        );

        // Costed of same ADT types
        assert_lub_eq(
            &mut reg,
            &Costed(Adt("Dog".to_string()).into()).into(),
            &Costed(Adt("Dog".to_string()).into()).into(),
            Costed(Adt("Dog".to_string()).into()),
        );

        // Mixed Stored and Costed with same ADT type
        assert_lub_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Costed(Adt("Dog".to_string()).into()).into(),
            Stored(Adt("Dog".to_string()).into()),
        );

        // Mixed Stored and Costed with related ADT types
        assert_lub_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Costed(Adt("Cat".to_string()).into()).into(),
            Stored(Adt("Mammals".to_string()).into()),
        );

        // Stored/Costed with regular ADT type
        assert_lub_eq(
            &mut reg,
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Adt("Cat".to_string()).into(),
            Adt("Mammals".to_string()),
        );
    }

    #[test]
    fn test_native_traits_with_adts() {
        let mut reg = setup_type_hierarchy();

        // EqHash with ADT types
        assert_lub_eq(
            &mut reg,
            &EqHash.into(),
            &Adt("Dog".to_string()).into(),
            EqHash,
        );

        // Concat with Array of ADTs
        assert_lub_eq(
            &mut reg,
            &Concat.into(),
            &Array(Adt("Dog".to_string()).into()).into(),
            Concat,
        );

        // Arithmetic with numeric type
        assert_lub_eq(&mut reg, &Arithmetic.into(), &I64.into(), Arithmetic);
    }

    #[test]
    fn test_complex_nested_types_lub() {
        let mut reg = setup_type_hierarchy();

        // Array of Optional ADTs
        assert_lub_eq(
            &mut reg,
            &Array(Optional(Adt("Dog".to_string()).into()).into()).into(),
            &Array(Optional(Adt("Cat".to_string()).into()).into()).into(),
            Array(Optional(Adt("Mammals".to_string()).into()).into()),
        );

        // Map with ADT keys and function values
        assert_lub_eq(
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
                    Adt("Bicycle".to_string()).into(),
                    Adt("Boat".to_string()).into(),
                )
                .into(),
            )
            .into(),
            Map(
                Adt("Dog".to_string()).into(),
                Closure(Nothing.into(), Adt("Boat".to_string()).into()).into(),
            ),
        );

        // Optional Stored Tuple of ADTs
        assert_lub_eq(
            &mut reg,
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
            &Optional(
                Stored(
                    Tuple(vec![
                        Adt("Cat".to_string()).into(),
                        Adt("Bicycle".to_string()).into(),
                    ])
                    .into(),
                )
                .into(),
            )
            .into(),
            Optional(
                Stored(
                    Tuple(vec![
                        Adt("Mammals".to_string()).into(),
                        Adt("LandVehicles".to_string()).into(),
                    ])
                    .into(),
                )
                .into(),
            ),
        );
    }

    #[test]
    fn test_map_function_compatibility_lub() {
        let mut reg = setup_type_hierarchy();

        // Map and Function compatibility
        assert_lub_eq(
            &mut reg,
            &Map(Adt("Dog".to_string()).into(), Adt("Car".to_string()).into()).into(),
            &Closure(
                Adt("Mammals".to_string()).into(),
                Optional(Adt("LandVehicles".to_string()).into()).into(),
            )
            .into(),
            Closure(
                Adt("Dog".to_string()).into(), // GLB of Dog and Mammals is Dog (contravariance)
                Optional(Adt("LandVehicles".to_string()).into()).into(), // LUB of Car? and LandVehicles?
            ),
        );

        // Array and Function compatibility
        assert_lub_eq(
            &mut reg,
            &Array(Adt("Dog".to_string()).into()).into(),
            &Closure(
                I64.into(),
                Optional(Adt("Mammals".to_string()).into()).into(),
            )
            .into(),
            Closure(
                I64.into(),
                Optional(Adt("Mammals".to_string()).into()).into(),
            ),
        );
    }
}
