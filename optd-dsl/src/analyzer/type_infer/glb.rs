use crate::analyzer::{
    hir::Identifier,
    types::{Type, TypeRegistry},
};
use std::collections::HashSet;

impl TypeRegistry {
    /// Finds the greatest lower bound (GLB) of two types.
    ///
    /// The greatest lower bound is the most general type that is a subtype of both input types.
    /// This operation is also known as the "meet" in type theory.
    ///
    /// # Arguments
    ///
    /// * `type1` - The first type
    /// * `type2` - The second type
    ///
    /// # Returns
    ///
    /// The greatest lower bound of the two types. Returns `Type::Nothing` if no common
    /// subtype exists, as Nothing is the bottom type in the type system.
    pub fn greatest_lower_bound(&self, type1: &Type, type2: &Type) -> Type {
        use Type::*;

        // If one type is a subtype of the other, the subtype is the GLB
        if self.is_subtype(type1, type2) {
            return type1.clone();
        }
        if self.is_subtype(type2, type1) {
            return type2.clone();
        }

        // Handle identical types
        if type1 == type2 {
            return type1.clone();
        }

        // Special case for Nothing (bottom type)
        if matches!(type1, Nothing) || matches!(type2, Nothing) {
            return Nothing;
        }

        // Special case for None and Optional types
        if matches!(type1, None) && matches!(type2, Optional(_)) {
            return None;
        }
        if matches!(type2, None) && matches!(type1, Optional(_)) {
            return None;
        }

        // Handle composite types with the same structure
        match (type1, type2) {
            // Arrays: find the GLB of element types
            (Array(elem1), Array(elem2)) => {
                let glb_elem = self.greatest_lower_bound(elem1, elem2);
                if matches!(glb_elem, Nothing) {
                    return Nothing;
                }
                return Array(Box::new(glb_elem));
            }

            // Tuples: find GLB for each pair of elements (if same length)
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let mut glb_elems = Vec::with_capacity(elems1.len());

                for (e1, e2) in elems1.iter().zip(elems2.iter()) {
                    let elem_glb = self.greatest_lower_bound(e1, e2);
                    if matches!(elem_glb, Nothing) {
                        return Nothing; // If any element pair has no GLB, the tuples have no GLB
                    }
                    glb_elems.push(elem_glb);
                }

                return Tuple(glb_elems);
            }

            // Maps: find GLB for keys and values
            (Map(key1, val1), Map(key2, val2)) => {
                let glb_key = self.greatest_lower_bound(key1, key2);
                let glb_val = self.greatest_lower_bound(val1, val2);

                if matches!(glb_key, Nothing) || matches!(glb_val, Nothing) {
                    return Nothing;
                }

                return Map(Box::new(glb_key), Box::new(glb_val));
            }

            // Optional types: find GLB of inner types
            (Optional(inner1), Optional(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                if matches!(glb_inner, Nothing) {
                    return Nothing;
                }
                return Optional(Box::new(glb_inner));
            }

            // Stored types: find GLB of inner types
            (Stored(inner1), Stored(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                if matches!(glb_inner, Nothing) {
                    return Nothing;
                }
                return Stored(Box::new(glb_inner));
            }

            // Costed types: find GLB of inner types
            (Costed(inner1), Costed(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                if matches!(glb_inner, Nothing) {
                    return Nothing;
                }
                return Costed(Box::new(glb_inner));
            }

            // For mixed Costed and Stored types
            (Costed(inner1), Stored(inner2)) | (Stored(inner1), Costed(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                if matches!(glb_inner, Nothing) {
                    return Nothing;
                }
                return Costed(Box::new(glb_inner));
            }

            // For function types, use covariance for parameters and contravariance for return types
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                // For covariant parameter types, we need the LUB
                let param_lub = self.least_upper_bound(param1, param2);

                // For contravariant return types, we need the GLB
                let ret_glb = self.greatest_lower_bound(ret1, ret2);
                if matches!(ret_glb, Nothing) {
                    return Nothing;
                }

                return Closure(Box::new(param_lub), Box::new(ret_glb));
            }

            // For ADTs, we need to find the most general common subtype
            (Adt(name1), Adt(name2)) => {
                // Try to find a common subtype in the type hierarchy
                if let Some(common_subtype) = self.find_common_subtype(name1, name2) {
                    return Adt(common_subtype);
                }
            }

            // Handle Optional and its inner type
            (Optional(inner), other) | (other, Optional(inner))
                if self.is_subtype(other, inner) =>
            {
                // If other is a subtype of the inner type, other is the GLB
                return other.clone();
            }

            // Handle Stored and its inner type
            (Stored(inner), other) | (other, Stored(inner)) if self.is_subtype(other, inner) => {
                // If other is a subtype of the inner type, Costed(other) is the GLB
                return Costed(Box::new(other.clone()));
            }

            // Handle Costed and its inner type
            (Costed(inner), other) | (other, Costed(inner)) if self.is_subtype(other, inner) => {
                // If other is a subtype of the inner type, Costed(other) is the GLB
                return Costed(Box::new(other.clone()));
            }

            // Native traits: check if there's a common implementation
            (_, Concat) | (Concat, _) => {
                // Find a common type that implements Concat, if any
                for common_type in [
                    String,
                    Array(Box::new(Universe)),
                    Map(Box::new(Universe), Box::new(Universe)),
                ] {
                    if self.is_subtype(type1, &common_type) && self.is_subtype(type2, &common_type)
                    {
                        return common_type;
                    }
                }
            }

            (_, EqHash) | (EqHash, _) => {
                // Find a common type that implements EqHash, if any
                for common_type in [I64, String, Bool, Unit, None] {
                    if self.is_subtype(type1, &common_type) && self.is_subtype(type2, &common_type)
                    {
                        return common_type;
                    }
                }
            }

            (_, Arithmetic) | (Arithmetic, _) => {
                // Find a common type that implements Arithmetic, if any
                for common_type in [I64, F64] {
                    if self.is_subtype(type1, &common_type) && self.is_subtype(type2, &common_type)
                    {
                        return common_type;
                    }
                }
            }

            _ => {}
        }

        // If no common subtype found, return Nothing (bottom type)
        Nothing
    }

    /// Finds a common subtype of two ADT identifiers in the type hierarchy.
    ///
    /// # Arguments
    ///
    /// * `name1` - The first ADT identifier
    /// * `name2` - The second ADT identifier
    ///
    /// # Returns
    ///
    /// Some(identifier) if a common subtype is found, None otherwise.
    fn find_common_subtype(&self, name1: &Identifier, name2: &Identifier) -> Option<Identifier> {
        // Get all subtypes of name1
        let subtypes1 = self.get_all_subtypes(name1);

        // Get all subtypes of name2
        let subtypes2 = self.get_all_subtypes(name2);

        // Find the most general common subtype
        // First collect the common subtypes
        let common_subtypes: Vec<&Identifier> = subtypes1
            .iter()
            .filter(|&st| subtypes2.contains(st))
            .collect();

        // If no common subtypes, return None
        if common_subtypes.is_empty() {
            return None;
        }

        // Find the most general one (the one that is a supertype of all others)
        let mut most_general = common_subtypes[0];

        for &subtype in &common_subtypes[1..] {
            if self.is_subtype(
                &Type::Adt(most_general.clone()),
                &Type::Adt(subtype.clone()),
            ) {
                most_general = subtype;
            }
        }

        Some(most_general.clone())
    }

    /// Gets all subtypes of an ADT identifier in the type hierarchy.
    ///
    /// # Arguments
    ///
    /// * `name` - The ADT identifier
    ///
    /// # Returns
    ///
    /// A HashSet containing all subtypes of the given ADT, including itself.
    fn get_all_subtypes(&self, name: &Identifier) -> HashSet<Identifier> {
        let mut result = HashSet::new();
        result.insert(name.clone());

        // Inner recursive function to collect subtypes
        fn collect_subtypes(
            registry: &TypeRegistry,
            name: &Identifier,
            result: &mut HashSet<Identifier>,
        ) {
            // Look through all parent-children relationships
            for (parent, children) in &registry.subtypes {
                // If this name is a parent
                if parent == name {
                    // Add all its children to the result
                    for child in children {
                        if result.insert(child.clone()) {
                            // Recursively collect subtypes of each child
                            collect_subtypes(registry, child, result);
                        }
                    }
                }
            }
        }

        collect_subtypes(self, name, &mut result);
        result
    }
}

#[cfg(test)]
mod greatest_lower_bound_tests {
    use super::*;
    use crate::{
        parser::ast::{Adt, Field, Type as AstType},
        utils::span::{Span, Spanned},
    };
    use Adt::*;

    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    fn create_product_adt(name: &str, fields: Vec<(&str, AstType)>) -> Adt {
        let spanned_fields: Vec<Spanned<Field>> = fields
            .into_iter()
            .map(|(field_name, field_type)| {
                spanned(Field {
                    name: spanned(field_name.to_string()),
                    ty: spanned(field_type),
                })
            })
            .collect();

        Product {
            name: spanned(name.to_string()),
            fields: spanned_fields,
        }
    }

    fn create_sum_adt(name: &str, variants: Vec<Adt>) -> Adt {
        Sum {
            name: spanned(name.to_string()),
            variants: variants.into_iter().map(spanned).collect(),
        }
    }

    #[test]
    fn test_glb_primitive_types() {
        let registry = TypeRegistry::default();

        // Same primitive types
        assert_eq!(
            registry.greatest_lower_bound(&Type::I64, &Type::I64),
            Type::I64
        );
        assert_eq!(
            registry.greatest_lower_bound(&Type::String, &Type::String),
            Type::String
        );
        assert_eq!(
            registry.greatest_lower_bound(&Type::Bool, &Type::Bool),
            Type::Bool
        );
        assert_eq!(
            registry.greatest_lower_bound(&Type::F64, &Type::F64),
            Type::F64
        );
        assert_eq!(
            registry.greatest_lower_bound(&Type::Unit, &Type::Unit),
            Type::Unit
        );

        // Different primitive types - should return Nothing as there's no common subtype
        assert_eq!(
            registry.greatest_lower_bound(&Type::I64, &Type::String),
            Type::Nothing
        );
        assert_eq!(
            registry.greatest_lower_bound(&Type::Bool, &Type::F64),
            Type::Nothing
        );

        // Special case for Nothing - now returns Nothing as expected
        assert_eq!(
            registry.greatest_lower_bound(&Type::Nothing, &Type::I64),
            Type::Nothing
        );
        assert_eq!(
            registry.greatest_lower_bound(&Type::I64, &Type::Nothing),
            Type::Nothing
        );

        // Special case for None and Optional types
        assert_eq!(
            registry.greatest_lower_bound(&Type::None, &Type::Optional(Box::new(Type::I64))),
            Type::None
        );
    }

    #[test]
    fn test_glb_composite_types() {
        let registry = TypeRegistry::default();

        // Array types with the same element type
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::I64)),
                &Type::Array(Box::new(Type::I64))
            ),
            Type::Array(Box::new(Type::I64))
        );

        // Array types with different element types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::I64)),
                &Type::Array(Box::new(Type::String))
            ),
            Type::Nothing
        );

        // Tuples with the same types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Tuple(vec![Type::I64, Type::String]),
                &Type::Tuple(vec![Type::I64, Type::String])
            ),
            Type::Tuple(vec![Type::I64, Type::String])
        );

        // Tuples with different lengths
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Tuple(vec![Type::I64, Type::String]),
                &Type::Tuple(vec![Type::I64])
            ),
            Type::Nothing
        );

        // Different composite types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Array(Box::new(Type::I64)),
                &Type::Tuple(vec![Type::I64, Type::String])
            ),
            Type::Nothing
        );
    }

    #[test]
    fn test_glb_stored_costed_types() {
        let registry = TypeRegistry::default();

        // Stored types with the same inner type
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Stored(Box::new(Type::I64)),
                &Type::Stored(Box::new(Type::I64))
            ),
            Type::Stored(Box::new(Type::I64))
        );

        // Costed types with the same inner type
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Costed(Box::new(Type::I64)),
                &Type::Costed(Box::new(Type::I64))
            ),
            Type::Costed(Box::new(Type::I64))
        );

        // Mix of Stored and Costed - should result in Costed
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Stored(Box::new(Type::I64)),
                &Type::Costed(Box::new(Type::I64))
            ),
            Type::Costed(Box::new(Type::I64))
        );

        // Stored and non-wrapped type
        assert_eq!(
            registry.greatest_lower_bound(&Type::Stored(Box::new(Type::I64)), &Type::I64),
            Type::Costed(Box::new(Type::I64))
        );
    }

    #[test]
    fn test_glb_adt_hierarchy() {
        let mut registry = TypeRegistry::default();

        // Create a vehicle hierarchy
        let vehicle = create_product_adt("Vehicle", vec![("wheels", AstType::Int64)]);
        let car = create_product_adt(
            "Car",
            vec![("wheels", AstType::Int64), ("doors", AstType::Int64)],
        );
        let truck = create_product_adt(
            "Truck",
            vec![("wheels", AstType::Int64), ("load", AstType::Float64)],
        );
        let vehicles_enum = create_sum_adt("Vehicles", vec![vehicle, car, truck]);

        // Register the ADTs
        registry.register_adt(&vehicles_enum).unwrap();

        // GLB of Car and Vehicle should be Nothing (no common subtype)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Adt("Car".to_string()),
                &Type::Adt("Vehicle".to_string())
            ),
            Type::Nothing
        );

        // GLB of Car and Vehicles should be Car (subtype)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Adt("Car".to_string()),
                &Type::Adt("Vehicles".to_string())
            ),
            Type::Adt("Car".to_string())
        );

        // GLB of Vehicle and Vehicles should be Vehicle (subtype)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Adt("Vehicle".to_string()),
                &Type::Adt("Vehicles".to_string())
            ),
            Type::Adt("Vehicle".to_string())
        );

        // GLB of Car and Truck should be Nothing (no common subtype)
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Adt("Car".to_string()),
                &Type::Adt("Truck".to_string())
            ),
            Type::Nothing
        );
    }

    #[test]
    fn test_glb_optional_types() {
        let registry = TypeRegistry::default();

        // Optional types with the same inner type
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::I64)),
                &Type::Optional(Box::new(Type::I64))
            ),
            Type::Optional(Box::new(Type::I64))
        );

        // None and Optional type
        assert_eq!(
            registry.greatest_lower_bound(&Type::None, &Type::Optional(Box::new(Type::I64))),
            Type::None
        );

        // Optional and its inner type
        assert_eq!(
            registry.greatest_lower_bound(&Type::Optional(Box::new(Type::I64)), &Type::I64),
            Type::I64
        );

        // Different optional types
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Optional(Box::new(Type::I64)),
                &Type::Optional(Box::new(Type::String))
            ),
            Type::Nothing
        );
    }

    #[test]
    fn test_glb_function_types() {
        let mut registry = TypeRegistry::default();

        // Create a type hierarchy for testing GLB with functions
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let cat = create_product_adt("Cat", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog, cat]);
        registry.register_adt(&animals_enum).unwrap();

        // Same function signatures
        assert_eq!(
            registry.greatest_lower_bound(
                &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
                &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
            ),
            Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
        );

        // Functions with param types in subtype relationship (params should use LUB)
        // GLB(Closure(Dog, Bool), Closure(Animals, Bool)) = Closure(Animals, Bool)
        let fn1 = Type::Closure(Box::new(Type::Adt("Dog".to_string())), Box::new(Type::Bool));
        let fn2 = Type::Closure(
            Box::new(Type::Adt("Animals".to_string())),
            Box::new(Type::Bool),
        );

        // The parameter type should be Animals (broader/more general)
        assert_eq!(
            registry.greatest_lower_bound(&fn1, &fn2),
            Type::Closure(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::Bool)
            )
        );

        // Functions with return types in subtype relationship (returns should use GLB)
        // GLB(Closure(I64, Animals), Closure(I64, Dog)) = Closure(I64, Dog)
        let fn3 = Type::Closure(
            Box::new(Type::I64),
            Box::new(Type::Adt("Animals".to_string())),
        );
        let fn4 = Type::Closure(Box::new(Type::I64), Box::new(Type::Adt("Dog".to_string())));

        // The return type should be Dog (narrower/more specific)
        assert_eq!(
            registry.greatest_lower_bound(&fn3, &fn4),
            Type::Closure(Box::new(Type::I64), Box::new(Type::Adt("Dog".to_string())))
        );
    }

    #[test]
    fn test_get_all_subtypes() {
        let mut registry = TypeRegistry::default();

        // Create hierarchy
        let animal = create_product_adt("Animal", vec![]);
        let mammal = create_product_adt("Mammal", vec![]);
        let bird = create_product_adt("Bird", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let cat = create_product_adt("Cat", vec![]);

        let mammals = create_sum_adt("Mammals", vec![mammal.clone(), dog.clone(), cat.clone()]);
        let animals = create_sum_adt(
            "Animals",
            vec![animal.clone(), mammals.clone(), bird.clone()],
        );

        registry.register_adt(&animals).unwrap();

        // Test get_all_subtypes
        let animal_subtypes = registry.get_all_subtypes(&"Animals".to_string());
        assert!(animal_subtypes.contains(&"Animals".to_string()));
        assert!(animal_subtypes.contains(&"Animal".to_string()));
        assert!(animal_subtypes.contains(&"Mammals".to_string()));
        assert!(animal_subtypes.contains(&"Mammal".to_string()));
        assert!(animal_subtypes.contains(&"Dog".to_string()));
        assert!(animal_subtypes.contains(&"Cat".to_string()));
        assert!(animal_subtypes.contains(&"Bird".to_string()));

        let mammal_subtypes = registry.get_all_subtypes(&"Mammals".to_string());
        assert!(mammal_subtypes.contains(&"Mammals".to_string()));
        assert!(mammal_subtypes.contains(&"Mammal".to_string()));
        assert!(mammal_subtypes.contains(&"Dog".to_string()));
        assert!(mammal_subtypes.contains(&"Cat".to_string()));
        assert!(!mammal_subtypes.contains(&"Bird".to_string()));
        assert!(!mammal_subtypes.contains(&"Animals".to_string()));
    }

    #[test]
    fn test_glb_identity() {
        // The GLB of a type with itself should be the type itself
        let registry = TypeRegistry::default();
        let types = [
            Type::I64,
            Type::String,
            Type::Bool,
            Type::F64,
            Type::Unit,
            Type::Universe,
            Type::Nothing,
            Type::None,
            Type::Array(Box::new(Type::I64)),
            Type::Tuple(vec![Type::I64, Type::String]),
            Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            Type::Optional(Box::new(Type::I64)),
            Type::Stored(Box::new(Type::I64)),
            Type::Costed(Box::new(Type::I64)),
            Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            Type::Concat,
            Type::EqHash,
            Type::Arithmetic,
        ];

        for ty in &types {
            assert_eq!(registry.greatest_lower_bound(ty, ty), ty.clone());
        }
    }

    #[test]
    fn test_glb_with_universe() {
        // The GLB of any type with Universe should be the type itself
        let registry = TypeRegistry::default();
        let types = [
            Type::I64,
            Type::String,
            Type::Bool,
            Type::F64,
            Type::Unit,
            Type::Nothing,
            Type::None,
            Type::Array(Box::new(Type::I64)),
            Type::Tuple(vec![Type::I64, Type::String]),
            Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            Type::Optional(Box::new(Type::I64)),
            Type::Stored(Box::new(Type::I64)),
            Type::Costed(Box::new(Type::I64)),
            Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            Type::Concat,
            Type::EqHash,
            Type::Arithmetic,
        ];

        for ty in &types {
            assert_eq!(
                registry.greatest_lower_bound(ty, &Type::Universe),
                ty.clone()
            );
            assert_eq!(
                registry.greatest_lower_bound(&Type::Universe, ty),
                ty.clone()
            );
        }
    }

    #[test]
    fn test_glb_with_nothing() {
        // The GLB of any type with Nothing should be Nothing
        let registry = TypeRegistry::default();
        let types = [
            Type::I64,
            Type::String,
            Type::Bool,
            Type::F64,
            Type::Unit,
            Type::Universe,
            Type::None,
            Type::Array(Box::new(Type::I64)),
            Type::Tuple(vec![Type::I64, Type::String]),
            Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            Type::Optional(Box::new(Type::I64)),
            Type::Stored(Box::new(Type::I64)),
            Type::Costed(Box::new(Type::I64)),
            Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            Type::Concat,
            Type::EqHash,
            Type::Arithmetic,
        ];

        for ty in &types {
            assert_eq!(
                registry.greatest_lower_bound(ty, &Type::Nothing),
                Type::Nothing
            );
            assert_eq!(
                registry.greatest_lower_bound(&Type::Nothing, ty),
                Type::Nothing
            );
        }
    }

    #[test]
    fn test_glb_native_traits() {
        let registry = TypeRegistry::default();

        // GLB of two types that implement the same trait
        // For EqHash
        assert_eq!(
            registry.greatest_lower_bound(&Type::I64, &Type::String),
            Type::Nothing
        );

        // For Arithmetic
        assert_eq!(
            registry.greatest_lower_bound(&Type::I64, &Type::F64),
            Type::Nothing
        );

        // For Concat
        assert_eq!(
            registry.greatest_lower_bound(&Type::String, &Type::Array(Box::new(Type::I64))),
            Type::Nothing
        );

        // GLB of a type with a trait it implements
        assert_eq!(
            registry.greatest_lower_bound(&Type::I64, &Type::Arithmetic),
            Type::I64
        );

        assert_eq!(
            registry.greatest_lower_bound(&Type::String, &Type::EqHash),
            Type::String
        );

        assert_eq!(
            registry.greatest_lower_bound(&Type::String, &Type::Concat),
            Type::String
        );
    }
}
