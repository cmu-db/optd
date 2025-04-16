use crate::analyzer::{
    hir::Identifier,
    types::{Type, TypeRegistry},
};
use std::collections::HashSet;

impl TypeRegistry {
    /// Finds the least upper bound (LUB) of two types.
    ///
    /// The least upper bound is the most specific type that is a supertype of both input types.
    /// This operation is also known as the "join" in type theory.
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
    pub fn least_upper_bound(&self, type1: &Type, type2: &Type) -> Type {
        use Type::*;

        match (type1, type2) {
            // Nothing is the bottom type.
            (Nothing, other) | (other, Nothing) => other.clone(),

            (Array(elem1), Array(elem2)) => {
                let lub_elem = self.least_upper_bound(elem1, elem2);
                return Array(Box::new(lub_elem));
            }

            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let lub_elems = elems1
                    .iter()
                    .zip(elems2.iter())
                    .map(|(e1, e2)| self.least_upper_bound(e1, e2))
                    .collect();
                Tuple(lub_elems)
            }

            (Map(key1, val1), Map(key2, val2)) => {
                let lub_key = self.least_upper_bound(key1, key2);
                let lub_val = self.least_upper_bound(val1, val2);
                Map(lub_key.into(), lub_val.into())
            }

            (Optional(inner1), Optional(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Optional(lub_inner.into())
            }

            (None, Optional(inner)) | (Optional(inner), None) => Optional(inner.clone()),

            (Stored(inner1), Stored(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Stored(lub_inner.into())
            }

            (Costed(inner1), Costed(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Costed(lub_inner.into())
            }

            // Mhh -----
            (Costed(inner1), Stored(inner2)) | (Stored(inner1), Costed(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Stored(lub_inner.into())
            }
            (Stored(inner), other) | (other, Stored(inner)) if self.is_subtype(inner, other) => {
                Stored(other.clone().into())
            }
            (Costed(inner), other) | (other, Costed(inner)) if self.is_subtype(inner, other) => {
                Stored(other.clone().into())
            }
            (Optional(inner), other) | (other, Optional(inner))
                if self.is_subtype(inner, other) =>
            {
                Optional(other.clone().into())
            }
            // Mhh -----
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_type = self.greatest_lower_bound(param1, param2);
                let ret_type = self.least_upper_bound(ret1, ret2);

                Closure(param_type.into(), ret_type.into())
            }

            (Adt(name1), Adt(name2)) => {
                if let Some(common_ancestor) = self.find_common_supertype(name1, name2) {
                    Adt(common_ancestor)
                } else {
                    Universe
                }
            }

            // Handle native trait types.
            // TODO: This is annoying as not always ordered!
            // Stored(A) <: A
            // B <: A
            // Stored(B) <: B
            // Stored(B) <: Stored(A) <: A
            // ->    B   -->   A <------- Stored(A)
            
            // A <: Option(A)
            (_, Concat) | (Concat, _)
                if self.is_subtype(type1, &Concat) && self.is_subtype(type2, &Concat) =>
            {
                Concat
            }

            (_, EqHash) | (EqHash, _)
                if self.is_subtype(type1, &EqHash) && self.is_subtype(type2, &EqHash) =>
            {
                EqHash
            }

            (_, Arithmetic) | (Arithmetic, _)
                if self.is_subtype(type1, &Arithmetic) && self.is_subtype(type2, &Arithmetic) =>
            {
                Arithmetic
            }

            _ => Universe,
        }
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
    fn find_common_supertype(&self, name1: &Identifier, name2: &Identifier) -> Option<Identifier> {
        let supertypes1 = self.get_all_supertypes(name1);
        let supertypes2 = self.get_all_supertypes(name2);

        let common_supertypes: Vec<&Identifier> = supertypes1
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
mod least_upper_bound_tests {
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
    fn test_lub_primitive_types() {
        let registry = TypeRegistry::default();

        // Same primitive types
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::I64),
            Type::I64
        );
        assert_eq!(
            registry.least_upper_bound(&Type::String, &Type::String),
            Type::String
        );
        assert_eq!(
            registry.least_upper_bound(&Type::Bool, &Type::Bool),
            Type::Bool
        );
        assert_eq!(
            registry.least_upper_bound(&Type::F64, &Type::F64),
            Type::F64
        );
        assert_eq!(
            registry.least_upper_bound(&Type::Unit, &Type::Unit),
            Type::Unit
        );

        // Different primitive types - should result in Universe
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::String),
            Type::Universe
        );
        assert_eq!(
            registry.least_upper_bound(&Type::Bool, &Type::F64),
            Type::Universe
        );

        // Special case for Nothing
        assert_eq!(
            registry.least_upper_bound(&Type::Nothing, &Type::I64),
            Type::I64
        );
        assert_eq!(
            registry.least_upper_bound(&Type::String, &Type::Nothing),
            Type::String
        );

        // Special case for None
        assert_eq!(
            registry.least_upper_bound(&Type::None, &Type::Optional(Box::new(Type::I64))),
            Type::Optional(Box::new(Type::I64))
        );
    }

    #[test]
    fn test_lub_composite_types() {
        let registry = TypeRegistry::default();

        // Array types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::I64)),
                &Type::Array(Box::new(Type::I64))
            ),
            Type::Array(Box::new(Type::I64))
        );

        // Tuples
        assert_eq!(
            registry.least_upper_bound(
                &Type::Tuple(vec![Type::I64, Type::String]),
                &Type::Tuple(vec![Type::I64, Type::String])
            ),
            Type::Tuple(vec![Type::I64, Type::String])
        );

        // Maps
        assert_eq!(
            registry.least_upper_bound(
                &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
                &Type::Map(Box::new(Type::String), Box::new(Type::I64))
            ),
            Type::Map(Box::new(Type::String), Box::new(Type::I64))
        );

        // Different types - should result in Universe
        assert_eq!(
            registry.least_upper_bound(
                &Type::Array(Box::new(Type::I64)),
                &Type::Tuple(vec![Type::I64, Type::String])
            ),
            Type::Universe
        );
    }

    #[test]
    fn test_lub_stored_costed_types() {
        let registry = TypeRegistry::default();

        // Stored types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::I64)),
                &Type::Stored(Box::new(Type::I64))
            ),
            Type::Stored(Box::new(Type::I64))
        );

        // Costed types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Costed(Box::new(Type::I64)),
                &Type::Costed(Box::new(Type::I64))
            ),
            Type::Costed(Box::new(Type::I64))
        );

        // Mix of Stored and Costed - should result in Stored
        assert_eq!(
            registry.least_upper_bound(
                &Type::Stored(Box::new(Type::I64)),
                &Type::Costed(Box::new(Type::I64))
            ),
            Type::Stored(Box::new(Type::I64))
        );

        // Stored and non-wrapped type
        assert_eq!(
            registry.least_upper_bound(&Type::Stored(Box::new(Type::I64)), &Type::I64),
            Type::Stored(Box::new(Type::I64))
        );
    }

    #[test]
    fn test_lub_native_trait_types() {
        let registry = TypeRegistry::default();

        // Types that both implement Concat
        assert_eq!(
            registry.least_upper_bound(&Type::String, &Type::Array(Box::new(Type::I64))),
            Type::Concat
        );

        // Types that both implement EqHash
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::String),
            Type::EqHash
        );

        // Types that both implement Arithmetic
        assert_eq!(
            registry.least_upper_bound(&Type::I64, &Type::F64),
            Type::Arithmetic
        );
    }

    #[test]
    fn test_lub_adt_hierarchy() {
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

        // LUB of Car and Truck should be Vehicles
        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Car".to_string()),
                &Type::Adt("Truck".to_string())
            ),
            Type::Adt("Vehicles".to_string())
        );

        // LUB of Car and Vehicle should be Vehicles
        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Car".to_string()),
                &Type::Adt("Vehicle".to_string())
            ),
            Type::Adt("Vehicles".to_string())
        );

        // LUB of Car and Vehicles should be Vehicles
        assert_eq!(
            registry.least_upper_bound(
                &Type::Adt("Car".to_string()),
                &Type::Adt("Vehicles".to_string())
            ),
            Type::Adt("Vehicles".to_string())
        );
    }

    #[test]
    fn test_lub_optional_types() {
        let registry = TypeRegistry::default();

        // Optional types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Optional(Box::new(Type::I64)),
                &Type::Optional(Box::new(Type::I64))
            ),
            Type::Optional(Box::new(Type::I64))
        );

        // None and Optional type
        assert_eq!(
            registry.least_upper_bound(&Type::None, &Type::Optional(Box::new(Type::I64))),
            Type::Optional(Box::new(Type::I64))
        );

        // Optional and its inner type
        assert_eq!(
            registry.least_upper_bound(&Type::Optional(Box::new(Type::I64)), &Type::I64),
            Type::Optional(Box::new(Type::I64))
        );

        // Different optional types
        assert_eq!(
            registry.least_upper_bound(
                &Type::Optional(Box::new(Type::I64)),
                &Type::Optional(Box::new(Type::F64))
            ),
            Type::Optional(Box::new(Type::Universe))
        );
    }

    #[test]
    fn test_lub_function_types() {
        let mut registry = TypeRegistry::default();

        // Create a type hierarchy for testing GLB with functions
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let cat = create_product_adt("Cat", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog, cat]);
        registry.register_adt(&animals_enum).unwrap();

        // Same function signatures
        assert_eq!(
            registry.least_upper_bound(
                &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
                &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
            ),
            Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
        );

        // Functions with related parameter types (params should use GLB)
        // Closure(Dog, Bool) join Closure(Cat, Bool) = Closure(Animals, Bool)
        // since Animals is a GLB of Dog and Cat
        let fn1 = Type::Closure(Box::new(Type::Adt("Dog".to_string())), Box::new(Type::Bool));
        let fn2 = Type::Closure(Box::new(Type::Adt("Cat".to_string())), Box::new(Type::Bool));
        assert_eq!(
            registry.least_upper_bound(&fn1, &fn2),
            Type::Closure(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::Bool)
            )
        );

        // Function with related return types
        // Closure(I64, Dog) join Closure(I64, Cat) = Closure(I64, Animals)
        let fn3 = Type::Closure(Box::new(Type::I64), Box::new(Type::Adt("Dog".to_string())));
        let fn4 = Type::Closure(Box::new(Type::I64), Box::new(Type::Adt("Cat".to_string())));
        assert_eq!(
            registry.least_upper_bound(&fn3, &fn4),
            Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::Adt("Animals".to_string()))
            )
        );

        // Function with both contravariant param types and covariant return types
        let fn5 = Type::Closure(Box::new(Type::Adt("Dog".to_string())), Box::new(Type::I64));
        let fn6 = Type::Closure(Box::new(Type::Adt("Cat".to_string())), Box::new(Type::F64));
        assert_eq!(
            registry.least_upper_bound(&fn5, &fn6),
            Type::Closure(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::Arithmetic)
            )
        );
    }
}
