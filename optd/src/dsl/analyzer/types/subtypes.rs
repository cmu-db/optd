use crate::dsl::analyzer::hir::Identifier;

use super::registry::{Type, TypeRegistry};
use std::collections::HashSet;

impl TypeRegistry {
    /// This is a convenience wrapper around `is_subtype_infer` that doesn't require
    /// tracking changes to unknown types.
    ///
    /// # Arguments
    ///
    /// * `child` - The potential subtype.
    /// * `parent` - The potential supertype.
    ///
    /// # Returns
    ///
    /// `true` if `child` is a subtype of `parent`, `false` otherwise.
    pub fn is_subtype(&mut self, child: &Type, parent: &Type) -> bool {
        self.is_subtype_infer(child, parent, &mut false)
    }

    /// Checks if a type is a subtype of another type.
    ///
    /// This method determines if `child` is a subtype of `parent` according to
    /// the language's type system rules. It handles primitive types, complex types
    /// with covariant and contravariant relationships, and user-defined ADTs.
    ///
    /// During type inference, this method can also refine unknown types to satisfy
    /// subtyping constraints. When an unknown type is encountered:
    /// - As a parent: It is updated to the least upper bound of itself and the child type.
    /// - As a child: It is simply substituted and checked against the parent type.
    ///
    /// The `bumped` parameter tracks whether any unknown types were modified
    /// during subtyping checks, which is used by the constraint solver to determine
    /// when to stop iterating.
    ///
    /// # Arguments
    ///
    /// * `child` - The potential subtype.
    /// * `parent` - The potential supertype.
    /// * `bumped` - Mutable flag to track if any unknown types were updated.
    ///
    /// # Returns
    ///
    /// `true` if `child` is a subtype of `parent` (or can be made a subtype by
    /// updating unknown types), `false` otherwise.
    ///
    /// # Note
    ///
    /// If no unknown types are involved, this behaves as a standard subtype check.
    pub fn is_subtype_infer(&mut self, child: &Type, parent: &Type, bumped: &mut bool) -> bool {
        let mut result;
        let mut local_bumped;

        // Continue checking until we reach stability (no more type updates).
        // Stop if we've reached a stable state (no more updates) or got a negative result.
        loop {
            local_bumped = false;
            result =
                self.is_subtype_infer_inner(child, parent, &mut local_bumped, &mut HashSet::new());
            *bumped |= local_bumped;

            if !local_bumped || !result {
                break;
            }
        }

        result
    }

    /// Inner implementation of is_subtype_infer with memoization for cycle detection.
    /// Cycles can occur in the type hierarchy with recursive ADTs and EqHash
    /// checks ADTs recursively deep.
    fn is_subtype_infer_inner(
        &mut self,
        child: &Type,
        parent: &Type,
        bumped: &mut bool,
        memo: &mut HashSet<(Type, Type)>,
    ) -> bool {
        use Type::*;

        // If we've already visited the pair (child, parent), return true to break cycles
        if !memo.insert((child.clone(), parent.clone())) {
            return true;
        }

        if child == parent {
            return true;
        }

        match (child, parent) {
            // Universe is the top type - everything is a subtype of Universe.
            (_, Universe) => true,

            // Nothing is the bottom type - it is a subtype of everything.
            (Nothing, _) => true,

            // If parent is unknown, bump up to LUB of child.
            (_, Unknown(id)) => {
                let parent = self.resolve_type(parent);
                let lub = self.least_upper_bound(child, &parent);

                if lub != parent {
                    self.resolved_unknown.insert(*id, lub);
                    *bumped = true;
                }

                true
            }

            // If child is unknown, check if its resolved type is a subtype of parent.
            (Unknown(_), _) => {
                let child = self.resolve_type(child);
                self.is_subtype_infer_inner(&child, parent, bumped, memo)
            }

            // Generics only match if they have strictly the same name.
            // Bounded generics are not yet supported.
            (Generic(gen1), Generic(gen2)) if gen1 == gen2 => true,

            // Stored and Costed type handling.
            (Stored(child_inner), Stored(parent_inner)) => {
                self.is_subtype_infer_inner(child_inner, parent_inner, bumped, memo)
            }
            (Costed(child_inner), Costed(parent_inner)) => {
                self.is_subtype_infer_inner(child_inner, parent_inner, bumped, memo)
            }
            (Costed(child_inner), Stored(parent_inner)) => {
                // Costed(A) is a subtype of Stored(A).
                self.is_subtype_infer_inner(child_inner, parent_inner, bumped, memo)
            }
            (Costed(child_inner), parent_inner) => {
                // Costed(A) is a subtype of A.
                self.is_subtype_infer_inner(child_inner, parent_inner, bumped, memo)
            }
            (Stored(child_inner), parent_inner) => {
                // Stored(A) is a subtype of A.
                self.is_subtype_infer_inner(child_inner, parent_inner, bumped, memo)
            }

            // Check transitive inheritance for ADTs.
            (Adt(child_name), Adt(parent_name)) => self.is_adt_child(child_name, parent_name),

            // Array covariance: Array[T] <: Array[U] if T <: U.
            (Array(child_elem), Array(parent_elem)) => {
                self.is_subtype_infer_inner(child_elem, parent_elem, bumped, memo)
            }

            // Map as a subtype of Function: Map(A, B) <: Closure(A, B?).
            (Map(key_type, val_type), Closure(param_type, ret_type)) => {
                self.is_subtype_infer_inner(param_type, key_type, bumped, memo)
                    && self.is_subtype_infer_inner(
                        &Optional(val_type.clone()),
                        ret_type,
                        bumped,
                        memo,
                    )
            }

            // Array as a subtype of Function: Array(B) <: Closure(I64, B?).
            (Array(elem_type), Closure(param_type, ret_type)) => {
                matches!(&**param_type, I64)
                    && self.is_subtype_infer_inner(
                        &Optional(elem_type.clone()),
                        ret_type,
                        bumped,
                        memo,
                    )
            }

            // Tuple covariance: (T1, T2, ...) <: (U1, U2, ...) if T1 <: U1, T2 <: U2, ...
            (Tuple(child_types), Tuple(parent_types))
                if child_types.len() == parent_types.len() =>
            {
                child_types
                    .iter()
                    .zip(parent_types.iter())
                    .all(|(c, p)| self.is_subtype_infer_inner(c, p, bumped, memo))
            }

            // Map covariance on values, contravariance on keys:
            // Map[K1, V1] <: Map[K2, V2] if K2 <: K1 and V1 <: V2.
            (Map(child_key, child_val), Map(parent_key, parent_val)) => {
                self.is_subtype_infer_inner(parent_key, child_key, bumped, memo)
                    && self.is_subtype_infer_inner(child_val, parent_val, bumped, memo)
            }

            // Function contravariance on args, covariance on return type:
            // (T1 -> U1) <: (T2 -> U2) if T2 <: T1 and U1 <: U2.
            (Closure(child_param, child_ret), Closure(parent_param, parent_ret)) => {
                self.is_subtype_infer_inner(parent_param, child_param, bumped, memo)
                    && self.is_subtype_infer_inner(child_ret, parent_ret, bumped, memo)
            }

            // Optional type covariance: Optional[T] <: Optional[U] if T <: U.
            (Optional(child_ty), Optional(parent_ty)) => {
                self.is_subtype_infer_inner(child_ty, parent_ty, bumped, memo)
            }
            // None <: Optional[Universe].
            (None, Optional(_)) => true,
            // Likewise, T <: Optional[T].
            (_, Optional(parent_inner)) => {
                self.is_subtype_infer_inner(child, parent_inner, bumped, memo)
            }

            // Native trait subtyping relationships

            // Concat trait implementations.
            (String, Concat) => true,
            (Array(_), Concat) => true,
            (Map(_, _), Concat) => true,

            // EqHash trait implementations.
            (I64, EqHash) => true,
            (String, EqHash) => true,
            (Bool, EqHash) => true,
            (Unit, EqHash) => true,
            (None, EqHash) => true,
            (Optional(inner), EqHash) => self.is_subtype_infer_inner(inner, &EqHash, bumped, memo),
            (Tuple(types), EqHash) => types
                .iter()
                .all(|t| self.is_subtype_infer_inner(t, &EqHash, bumped, memo)),
            (Adt(name), EqHash) => {
                // Product ADTs with all fields satisfying EqHash also satisfy EqHash.
                if let Some(fields) = self.product_fields.get(name).cloned() {
                    fields.iter().all(|field| {
                        let ty = self.get_product_field_type(name, &field.name).unwrap();
                        self.is_subtype_infer_inner(&ty, &EqHash, bumped, memo)
                    })
                } else if let Some(variants) = self.subtypes.get(name).cloned() {
                    // Sum types (enums) satisfy EqHash if all variants satisfy EqHash.
                    variants.iter().all(|variant| {
                        self.is_subtype_infer_inner(&Adt(variant.clone()), &EqHash, bumped, memo)
                    })
                } else {
                    false
                }
            }

            // Arithmetic trait implementations.
            (I64, Arithmetic) => true,
            (F64, Arithmetic) => true,

            _ => false,
        }
    }

    pub fn is_adt_child(&self, child_name: &Identifier, parent_name: &Identifier) -> bool {
        if child_name == parent_name {
            return true;
        }

        self.subtypes
            .get(parent_name)
            .cloned()
            .unwrap()
            .iter()
            .any(|subtype_child_name| self.is_adt_child(child_name, subtype_child_name))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::dsl::{
        analyzer::types::registry::type_registry_tests::{
            create_product_adt, create_sum_adt, spanned,
        },
        parser::ast::Type as AstType,
    };

    #[test]
    fn test_stored_and_costed_types() {
        let mut registry = TypeRegistry::default();

        // Test Stored type as a subtype of the inner type
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::I64)), &Type::I64));

        // Test Costed type as a subtype of Stored type
        assert!(registry.is_subtype(
            &Type::Costed(Box::new(Type::I64)),
            &Type::Stored(Box::new(Type::I64))
        ));

        // Test Costed type as a subtype of the inner type (transitivity)
        assert!(registry.is_subtype(&Type::Costed(Box::new(Type::I64)), &Type::I64));

        // Test Stored type covariance
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        assert!(adts_registry.is_subtype(
            &Type::Stored(Box::new(Type::Adt("Dog".to_string()))),
            &Type::Stored(Box::new(Type::Adt("Animals".to_string())))
        ));

        // Test Costed type covariance
        assert!(adts_registry.is_subtype(
            &Type::Costed(Box::new(Type::Adt("Dog".to_string()))),
            &Type::Costed(Box::new(Type::Adt("Animals".to_string())))
        ));

        // Test the inheritance relationship: Costed(Dog) <: Stored(Animals)
        assert!(adts_registry.is_subtype(
            &Type::Costed(Box::new(Type::Adt("Dog".to_string()))),
            &Type::Stored(Box::new(Type::Adt("Animals".to_string())))
        ));

        // Test nested Stored/Costed types
        assert!(adts_registry.is_subtype(
            &Type::Stored(Box::new(Type::Costed(Box::new(Type::Adt(
                "Dog".to_string()
            ))))),
            &Type::Stored(Box::new(Type::Adt("Animals".to_string())))
        ));

        // Test with Array of Stored/Costed types
        assert!(adts_registry.is_subtype(
            &Type::Array(Box::new(Type::Costed(Box::new(Type::Adt(
                "Dog".to_string()
            ))))),
            &Type::Array(Box::new(Type::Stored(Box::new(Type::Adt(
                "Animals".to_string()
            )))))
        ));
    }

    #[test]
    fn test_primitive_type_equality() {
        let mut registry = TypeRegistry::default();

        // Same primitive types should be subtypes of each other
        assert!(registry.is_subtype(&Type::I64, &Type::I64));
        assert!(registry.is_subtype(&Type::Bool, &Type::Bool));
        assert!(registry.is_subtype(&Type::String, &Type::String));
        assert!(registry.is_subtype(&Type::F64, &Type::F64));
        assert!(registry.is_subtype(&Type::Unit, &Type::Unit));
        assert!(registry.is_subtype(&Type::Universe, &Type::Universe));

        // Different primitive types should not be subtypes
        assert!(!registry.is_subtype(&Type::I64, &Type::Bool));
        assert!(!registry.is_subtype(&Type::String, &Type::I64));
        assert!(!registry.is_subtype(&Type::F64, &Type::I64));
        assert!(!registry.is_subtype(&Type::Unit, &Type::Bool));

        // All types should be subtypes of Universe
        assert!(registry.is_subtype(&Type::I64, &Type::Universe));
        assert!(registry.is_subtype(&Type::Bool, &Type::Universe));
        assert!(registry.is_subtype(&Type::String, &Type::Universe));
        assert!(registry.is_subtype(&Type::F64, &Type::Universe));
        assert!(registry.is_subtype(&Type::Unit, &Type::Universe));
    }

    #[test]
    fn test_array_subtyping() {
        let mut registry = TypeRegistry::default();

        // Same type arrays
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::I64)),
            &Type::Array(Box::new(Type::I64))
        ));

        // Different type arrays
        assert!(!registry.is_subtype(
            &Type::Array(Box::new(Type::I64)),
            &Type::Array(Box::new(Type::Bool))
        ));

        // Test nested arrays
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::Array(Box::new(Type::I64)))),
            &Type::Array(Box::new(Type::Array(Box::new(Type::I64))))
        ));

        // Array of any type is subtype of Universe
        assert!(registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Universe));

        // Array with inheritance (will be tested more with ADTs)
        let mut adts_registry = TypeRegistry::default();
        let vehicle = create_product_adt("Vehicle", vec![]);
        let car = create_product_adt("Car", vec![]);
        let vehicles_enum = create_sum_adt("Vehicles", vec![vehicle, car]);
        adts_registry.register_adt(&vehicles_enum).unwrap();

        assert!(adts_registry.is_subtype(
            &Type::Array(Box::new(Type::Adt("Car".to_string()))),
            &Type::Array(Box::new(Type::Adt("Vehicles".to_string())))
        ));
    }

    #[test]
    fn test_tuple_subtyping() {
        let mut registry = TypeRegistry::default();

        // Same type tuples
        assert!(registry.is_subtype(
            &Type::Tuple(vec![Type::I64, Type::Bool]),
            &Type::Tuple(vec![Type::I64, Type::Bool])
        ));

        // Different type tuples
        assert!(!registry.is_subtype(
            &Type::Tuple(vec![Type::I64, Type::Bool]),
            &Type::Tuple(vec![Type::Bool, Type::I64])
        ));

        // Different length tuples
        assert!(!registry.is_subtype(
            &Type::Tuple(vec![Type::I64, Type::Bool]),
            &Type::Tuple(vec![Type::I64, Type::Bool, Type::String])
        ));

        // Empty tuples
        assert!(registry.is_subtype(&Type::Tuple(vec![]), &Type::Tuple(vec![])));

        // All tuples are subtypes of Universe
        assert!(registry.is_subtype(&Type::Tuple(vec![Type::I64, Type::Bool]), &Type::Universe));

        // Nested tuples
        assert!(registry.is_subtype(
            &Type::Tuple(vec![Type::I64, Type::Tuple(vec![Type::Bool, Type::String])]),
            &Type::Tuple(vec![Type::I64, Type::Tuple(vec![Type::Bool, Type::String])])
        ));
    }

    #[test]
    fn test_map_subtyping() {
        // Setup basic registry
        let mut registry = TypeRegistry::default();

        // Same type maps
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Map(Box::new(Type::String), Box::new(Type::I64))
        ));

        // Different key types
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Map(Box::new(Type::I64), Box::new(Type::I64))
        ));

        // Different value types
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Map(Box::new(Type::String), Box::new(Type::Bool))
        ));

        // All maps are subtypes of Universe
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Universe
        ));

        // Create a registry with ADTs to test variance
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        // Test contravariance of map keys:
        // Map(Animals, String) <: Map(Dog, String) because Dog <: Animals
        assert!(adts_registry.is_subtype(
            &Type::Map(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::String)
            ),
            &Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::String)
            )
        ));

        // Test covariance of map values:
        // Map(String, Dog) <: Map(String, Animals) because Dog <: Animals
        assert!(adts_registry.is_subtype(
            &Type::Map(
                Box::new(Type::String),
                Box::new(Type::Adt("Dog".to_string()))
            ),
            &Type::Map(
                Box::new(Type::String),
                Box::new(Type::Adt("Animals".to_string()))
            )
        ));
    }

    #[test]
    fn test_closure_subtyping() {
        let mut registry = TypeRegistry::default();

        // Same function signatures
        assert!(registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
        ));

        // Contravariant parameter types - narrower param type is not a subtype
        assert!(!registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            &Type::Closure(Box::new(Type::F64), Box::new(Type::Bool))
        ));

        // All closures are subtypes of Universe
        assert!(registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            &Type::Universe
        ));

        // Contravariant parameter types - broader param type is a subtype
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        // (Animals -> Bool) <: (Dog -> Bool) because Dog <: Animals (contravariance)
        assert!(adts_registry.is_subtype(
            &Type::Closure(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::Bool)
            ),
            &Type::Closure(Box::new(Type::Adt("Dog".to_string())), Box::new(Type::Bool))
        ));

        // Covariant return types
        // (Int64 -> Dog) <: (Int64 -> Animals) because Dog <: Animals
        assert!(adts_registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Adt("Dog".to_string()))),
            &Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::Adt("Animals".to_string()))
            )
        ));
    }

    #[test]
    fn test_adt_registration_and_subtyping() {
        let mut registry = TypeRegistry::default();

        // Create a simple Shape hierarchy
        let shape = create_product_adt("Shape", vec![("area", AstType::Float64)]);
        let circle = create_product_adt(
            "Circle",
            vec![("radius", AstType::Float64), ("area", AstType::Float64)],
        );
        let rectangle = create_product_adt(
            "Rectangle",
            vec![
                ("width", AstType::Float64),
                ("height", AstType::Float64),
                ("area", AstType::Float64),
            ],
        );

        // Create an enum for shapes
        let shapes_enum = create_sum_adt("Shapes", vec![shape, circle, rectangle]);

        // Register the ADT
        registry.register_adt(&shapes_enum).unwrap();

        // Test subtypes relationship
        assert!(registry.is_subtype(
            &Type::Adt("Circle".to_string()),
            &Type::Adt("Shapes".to_string())
        ));

        assert!(registry.is_subtype(
            &Type::Adt("Rectangle".to_string()),
            &Type::Adt("Shapes".to_string())
        ));

        assert!(registry.is_subtype(
            &Type::Adt("Shape".to_string()),
            &Type::Adt("Shapes".to_string())
        ));

        // Same type should be a subtype of itself
        assert!(registry.is_subtype(
            &Type::Adt("Shapes".to_string()),
            &Type::Adt("Shapes".to_string())
        ));

        // All ADTs are subtypes of Universe
        assert!(registry.is_subtype(&Type::Adt("Shapes".to_string()), &Type::Universe));

        // Non-subtypes should return false
        assert!(!registry.is_subtype(
            &Type::Adt("Shapes".to_string()),
            &Type::Adt("Circle".to_string())
        ));

        assert!(!registry.is_subtype(
            &Type::Adt("Circle".to_string()),
            &Type::Adt("Rectangle".to_string())
        ));
    }

    #[test]
    fn test_universe_as_top_type() {
        let mut registry = TypeRegistry::default();

        // Check that Universe is a supertype of all primitive types
        assert!(registry.is_subtype(&Type::I64, &Type::Universe));
        assert!(registry.is_subtype(&Type::String, &Type::Universe));
        assert!(registry.is_subtype(&Type::Bool, &Type::Universe));
        assert!(registry.is_subtype(&Type::F64, &Type::Universe));
        assert!(registry.is_subtype(&Type::Unit, &Type::Universe));

        // Check that Universe is a supertype of all complex types
        assert!(registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Universe));
        assert!(registry.is_subtype(&Type::Tuple(vec![Type::I64, Type::Bool]), &Type::Universe));
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Universe
        ));
        assert!(registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            &Type::Universe
        ));

        // Check that Universe is a supertype of Stored and Costed types
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::I64)), &Type::Universe));
        assert!(registry.is_subtype(&Type::Costed(Box::new(Type::I64)), &Type::Universe));

        // But Universe is not a subtype of any other type
        assert!(!registry.is_subtype(&Type::Universe, &Type::I64));
        assert!(!registry.is_subtype(&Type::Universe, &Type::String));
        assert!(!registry.is_subtype(&Type::Universe, &Type::Array(Box::new(Type::I64))));
    }

    #[test]
    fn test_nothing_as_bottom_type() {
        let mut registry = TypeRegistry::default();

        // Nothing is a subtype of all primitive types
        assert!(registry.is_subtype(&Type::Nothing, &Type::I64));
        assert!(registry.is_subtype(&Type::Nothing, &Type::String));
        assert!(registry.is_subtype(&Type::Nothing, &Type::Bool));
        assert!(registry.is_subtype(&Type::Nothing, &Type::F64));
        assert!(registry.is_subtype(&Type::Nothing, &Type::Unit));
        assert!(registry.is_subtype(&Type::Nothing, &Type::Universe));

        // Nothing is a subtype of complex types
        assert!(registry.is_subtype(&Type::Nothing, &Type::Array(Box::new(Type::I64))));
        assert!(registry.is_subtype(&Type::Nothing, &Type::Tuple(vec![Type::I64, Type::Bool])));
        assert!(registry.is_subtype(
            &Type::Nothing,
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
        ));

        // But no type is a subtype of Nothing (except Nothing itself)
        assert!(!registry.is_subtype(&Type::I64, &Type::Nothing));
        assert!(!registry.is_subtype(&Type::Bool, &Type::Nothing));
        assert!(!registry.is_subtype(&Type::Universe, &Type::Nothing));
        assert!(!registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Nothing));
    }

    #[test]
    fn test_generic_subtyping() {
        let mut registry = TypeRegistry::default();

        // Generics are only subtypes of themselves (same name)
        assert!(registry.is_subtype(
            &Type::Generic("T".to_string()),
            &Type::Generic("T".to_string())
        ));

        // Different named generics are not subtypes
        assert!(!registry.is_subtype(
            &Type::Generic("T".to_string()),
            &Type::Generic("U".to_string())
        ));

        // All generics are subtypes of Universe
        assert!(registry.is_subtype(&Type::Generic("T".to_string()), &Type::Universe));

        // Nothing is a subtype of any generic
        assert!(registry.is_subtype(&Type::Nothing, &Type::Generic("T".to_string())));

        // Generic is not a subtype of concrete types
        assert!(!registry.is_subtype(&Type::Generic("T".to_string()), &Type::I64));

        // Concrete types are not subtypes of generics
        assert!(!registry.is_subtype(&Type::I64, &Type::Generic("T".to_string())));

        // Test with generic in container types
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::Generic("T".to_string()))),
            &Type::Array(Box::new(Type::Generic("T".to_string())))
        ));

        // Different generics in container types
        assert!(!registry.is_subtype(
            &Type::Array(Box::new(Type::Generic("T".to_string()))),
            &Type::Array(Box::new(Type::Generic("U".to_string())))
        ));
    }

    #[test]
    fn test_none_subtyping() {
        let mut registry = TypeRegistry::default();

        // Test None as a subtype of any Optional type
        assert!(registry.is_subtype(&Type::None, &Type::Optional(Box::new(Type::I64))));
        assert!(registry.is_subtype(&Type::None, &Type::Optional(Box::new(Type::String))));
        assert!(registry.is_subtype(&Type::None, &Type::Optional(Box::new(Type::Bool))));
        assert!(registry.is_subtype(&Type::None, &Type::Optional(Box::new(Type::F64))));
        assert!(registry.is_subtype(&Type::None, &Type::Optional(Box::new(Type::Unit))));

        // Test None with complex Optional types
        assert!(registry.is_subtype(
            &Type::None,
            &Type::Optional(Box::new(Type::Array(Box::new(Type::I64))))
        ));

        // Test that None is not a subtype of non-Optional types
        assert!(!registry.is_subtype(&Type::None, &Type::I64));
        assert!(!registry.is_subtype(&Type::None, &Type::String));

        // None is still a subtype of Universe (as all types are)
        assert!(registry.is_subtype(&Type::None, &Type::Universe));

        // None is not equal to Nothing
        assert!(!registry.is_subtype(&Type::None, &Type::Nothing));
        assert!(registry.is_subtype(&Type::Nothing, &Type::None));
    }

    #[test]
    fn test_type_optional_subtyping() {
        let mut registry = TypeRegistry::default();

        // Test that a type is a subtype of its corresponding optional type
        assert!(registry.is_subtype(&Type::I64, &Type::Optional(Box::new(Type::I64))));
        assert!(registry.is_subtype(&Type::String, &Type::Optional(Box::new(Type::String))));
        assert!(registry.is_subtype(&Type::Bool, &Type::Optional(Box::new(Type::Bool))));

        // Test with nested types
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::I64)),
            &Type::Optional(Box::new(Type::Array(Box::new(Type::I64))))
        ));

        // Test with inheritance
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        // Dog <: Optional<Dog>
        assert!(adts_registry.is_subtype(
            &Type::Adt("Dog".to_string()),
            &Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        ));

        // Dog <: Optional<Animals> (transitivity)
        assert!(adts_registry.is_subtype(
            &Type::Adt("Dog".to_string()),
            &Type::Optional(Box::new(Type::Adt("Animals".to_string())))
        ));

        // Test that non-subtypes remain non-subtypes when wrapped in Optional
        assert!(!adts_registry.is_subtype(
            &Type::Adt("Animals".to_string()),
            &Type::Optional(Box::new(Type::Adt("Dog".to_string())))
        ));
    }

    #[test]
    fn test_complex_nested_type_hierarchy() {
        let mut registry = TypeRegistry::default();

        // Create a complex type hierarchy for vehicles
        let vehicle = create_product_adt("Vehicle", vec![("wheels", AstType::Int64)]);

        let car = create_product_adt(
            "Car",
            vec![("wheels", AstType::Int64), ("doors", AstType::Int64)],
        );

        let sports_car = create_product_adt(
            "SportsCar",
            vec![
                ("wheels", AstType::Int64),
                ("doors", AstType::Int64),
                ("top_speed", AstType::Float64),
            ],
        );

        let truck = create_product_adt(
            "Truck",
            vec![
                ("wheels", AstType::Int64),
                ("load_capacity", AstType::Float64),
            ],
        );

        // First level enum: Cars
        let cars_enum = create_sum_adt("Cars", vec![car, sports_car]);

        // Second level enum: Vehicles
        let vehicles_enum = create_sum_adt("Vehicles", vec![vehicle, cars_enum, truck]);

        // Register the ADT
        registry.register_adt(&vehicles_enum).unwrap();

        // Test direct subtyping relationships
        assert!(registry.is_subtype(
            &Type::Adt("Car".to_string()),
            &Type::Adt("Cars".to_string())
        ));

        assert!(registry.is_subtype(
            &Type::Adt("SportsCar".to_string()),
            &Type::Adt("Cars".to_string())
        ));

        assert!(registry.is_subtype(
            &Type::Adt("Cars".to_string()),
            &Type::Adt("Vehicles".to_string())
        ));

        assert!(registry.is_subtype(
            &Type::Adt("Truck".to_string()),
            &Type::Adt("Vehicles".to_string())
        ));

        // Test transitive subtyping
        assert!(registry.is_subtype(
            &Type::Adt("SportsCar".to_string()),
            &Type::Adt("Vehicles".to_string())
        ));

        // All vehicle types are subtypes of Universe
        assert!(registry.is_subtype(&Type::Adt("Vehicles".to_string()), &Type::Universe));
        assert!(registry.is_subtype(&Type::Adt("Cars".to_string()), &Type::Universe));
        assert!(registry.is_subtype(&Type::Adt("SportsCar".to_string()), &Type::Universe));

        // Test negative cases
        assert!(!registry.is_subtype(
            &Type::Adt("Vehicles".to_string()),
            &Type::Adt("Cars".to_string())
        ));

        assert!(!registry.is_subtype(
            &Type::Adt("Cars".to_string()),
            &Type::Adt("SportsCar".to_string())
        ));

        assert!(!registry.is_subtype(
            &Type::Adt("Truck".to_string()),
            &Type::Adt("Cars".to_string())
        ));
    }

    #[test]
    fn test_native_trait_concat() {
        let mut registry = TypeRegistry::default();

        // Test types that should implement Concat
        assert!(registry.is_subtype(&Type::String, &Type::Concat));
        assert!(registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Concat));
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Concat
        ));

        // Test nested types that implement Concat
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::Array(Box::new(Type::I64)))),
            &Type::Concat
        ));

        // Test types that should not implement Concat
        assert!(!registry.is_subtype(&Type::I64, &Type::Concat));
        assert!(!registry.is_subtype(&Type::Bool, &Type::Concat));
        assert!(!registry.is_subtype(&Type::F64, &Type::Concat));
        assert!(!registry.is_subtype(&Type::Unit, &Type::Concat));
        assert!(!registry.is_subtype(&Type::None, &Type::Concat));
        assert!(!registry.is_subtype(&Type::Tuple(vec![Type::I64, Type::String]), &Type::Concat));
        assert!(!registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::String)),
            &Type::Concat
        ));

        // Special types
        assert!(registry.is_subtype(&Type::Nothing, &Type::Concat));
        assert!(!registry.is_subtype(&Type::Concat, &Type::Nothing));
        assert!(registry.is_subtype(&Type::Concat, &Type::Universe));

        // Stored/Costed with Concat-compatible inner types
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::String)), &Type::String));
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::String)), &Type::Concat));
        assert!(registry.is_subtype(&Type::Costed(Box::new(Type::String)), &Type::Concat));
    }

    #[test]
    fn test_native_trait_eqhash() {
        let mut registry = TypeRegistry::default();

        // Test primitive types that should implement EqHash
        assert!(registry.is_subtype(&Type::I64, &Type::EqHash));
        assert!(registry.is_subtype(&Type::String, &Type::EqHash));
        assert!(registry.is_subtype(&Type::Bool, &Type::EqHash));
        assert!(registry.is_subtype(&Type::Unit, &Type::EqHash));
        assert!(registry.is_subtype(&Type::None, &Type::EqHash));

        // Test tuple types with all EqHash elements
        assert!(registry.is_subtype(
            &Type::Tuple(vec![Type::I64, Type::String, Type::Bool]),
            &Type::EqHash
        ));

        // Mixed tuple with a non-EqHash type should not implement EqHash
        assert!(!registry.is_subtype(
            &Type::Tuple(vec![
                Type::I64,
                Type::Closure(Box::new(Type::I64), Box::new(Type::Bool))
            ]),
            &Type::EqHash
        ));

        // Test empty tuple (should implement EqHash)
        assert!(registry.is_subtype(&Type::Tuple(vec![]), &Type::EqHash));

        // Test types that should not implement EqHash
        assert!(!registry.is_subtype(&Type::F64, &Type::EqHash)); // Floating point is not guaranteed equality
        assert!(!registry.is_subtype(
            &Type::Closure(Box::new(Type::I64), Box::new(Type::Bool)),
            &Type::EqHash
        ));
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::EqHash
        ));

        // Special types
        assert!(registry.is_subtype(&Type::Nothing, &Type::EqHash));
        assert!(!registry.is_subtype(&Type::EqHash, &Type::Nothing));
        assert!(registry.is_subtype(&Type::EqHash, &Type::Universe));
    }

    #[test]
    fn test_native_trait_arithmetic() {
        let mut registry = TypeRegistry::default();

        // Test types that should implement Arithmetic
        assert!(registry.is_subtype(&Type::I64, &Type::Arithmetic));
        assert!(registry.is_subtype(&Type::F64, &Type::Arithmetic));

        // Test types that should not implement Arithmetic
        assert!(!registry.is_subtype(&Type::String, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::Bool, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::Unit, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::None, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::Tuple(vec![Type::I64, Type::F64]), &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Arithmetic));
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Arithmetic
        ));

        // Special types
        assert!(registry.is_subtype(&Type::Nothing, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::Arithmetic, &Type::Nothing));
        assert!(registry.is_subtype(&Type::Arithmetic, &Type::Universe));

        // Stored/Costed with Arithmetic-compatible inner types
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::I64)), &Type::I64));
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::I64)), &Type::Arithmetic));
        assert!(registry.is_subtype(&Type::Costed(Box::new(Type::F64)), &Type::Arithmetic));
    }

    #[test]
    fn test_adt_eqhash() {
        // Create a registry with ADTs
        let mut registry = TypeRegistry::default();

        // Create ADTs with EqHash-compatible fields
        let point = create_product_adt("Point", vec![("x", AstType::Int64), ("y", AstType::Int64)]);

        // Create ADT with mixed field types (some EqHash, some not)
        let complex_shape = create_product_adt(
            "ComplexShape",
            vec![
                ("name", AstType::String),
                (
                    "transform",
                    AstType::Closure(spanned(AstType::Int64), spanned(AstType::Int64)),
                ),
            ],
        );

        // Create sum type with all variants satisfying EqHash
        let shape1 = create_product_adt("Circle", vec![("radius", AstType::Int64)]);
        let shape2 = create_product_adt(
            "Rectangle",
            vec![("width", AstType::Int64), ("height", AstType::Int64)],
        );
        let shapes = create_sum_adt("Shape", vec![shape1, shape2]);

        // Register the ADTs
        registry.register_adt(&point).unwrap();
        registry.register_adt(&complex_shape).unwrap();
        registry.register_adt(&shapes).unwrap();

        // Test EqHash relationships

        // Point should satisfy EqHash since all fields (x, y) are Int64 which satisfies EqHash
        assert!(registry.is_subtype(&Type::Adt("Point".to_string()), &Type::EqHash));

        // ComplexShape should not satisfy EqHash since it has a Closure field which doesn't satisfy EqHash
        assert!(!registry.is_subtype(&Type::Adt("ComplexShape".to_string()), &Type::EqHash));

        // Circle and Rectangle should satisfy EqHash
        assert!(registry.is_subtype(&Type::Adt("Circle".to_string()), &Type::EqHash));
        assert!(registry.is_subtype(&Type::Adt("Rectangle".to_string()), &Type::EqHash));

        // Shape (sum type) should satisfy EqHash since all variants satisfy EqHash
        assert!(registry.is_subtype(&Type::Adt("Shape".to_string()), &Type::EqHash));
    }

    #[test]
    fn test_recursive_adt_with_native_traits() {
        // Create a registry with a recursive ADT
        let mut registry = TypeRegistry::default();

        // Create a recursive List type
        let list_node = create_product_adt(
            "ListNode",
            vec![
                ("value", AstType::Int64),
                (
                    "next",
                    AstType::Questioned(spanned(AstType::Identifier("ListNode".to_string()))),
                ),
            ],
        );

        // Register the ADT
        registry.register_adt(&list_node).unwrap();

        // Test relationships with native traits

        // ListNode should satisfy EqHash since both Int64 and Optional<ListNode> satisfy EqHash
        assert!(registry.is_subtype(&Type::Adt("ListNode".to_string()), &Type::EqHash));

        // ListNode should not satisfy Concat or Arithmetic
        assert!(!registry.is_subtype(&Type::Adt("ListNode".to_string()), &Type::Concat));
        assert!(!registry.is_subtype(&Type::Adt("ListNode".to_string()), &Type::Arithmetic));
    }

    #[test]
    fn test_multiple_native_traits() {
        let mut registry = TypeRegistry::default();

        // Test which types satisfy multiple traits

        // Int64 satisfies both EqHash and Arithmetic
        assert!(registry.is_subtype(&Type::I64, &Type::EqHash));
        assert!(registry.is_subtype(&Type::I64, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::I64, &Type::Concat));

        // String satisfies both EqHash and Concat
        assert!(registry.is_subtype(&Type::String, &Type::EqHash));
        assert!(registry.is_subtype(&Type::String, &Type::Concat));
        assert!(!registry.is_subtype(&Type::String, &Type::Arithmetic));

        // Float64 only satisfies Arithmetic
        assert!(registry.is_subtype(&Type::F64, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::F64, &Type::EqHash));
        assert!(!registry.is_subtype(&Type::F64, &Type::Concat));

        // Bool only satisfies EqHash
        assert!(registry.is_subtype(&Type::Bool, &Type::EqHash));
        assert!(!registry.is_subtype(&Type::Bool, &Type::Arithmetic));
        assert!(!registry.is_subtype(&Type::Bool, &Type::Concat));

        // Array satisfies only Concat
        assert!(registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Concat));
        assert!(!registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::EqHash));
        assert!(!registry.is_subtype(&Type::Array(Box::new(Type::I64)), &Type::Arithmetic));
    }

    #[test]
    fn test_collection_function_subtyping() {
        let mut registry = TypeRegistry::default();

        // Test Map as a subtype of Function
        // Map(String, I64) <: Closure(String, Optional<I64>)
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Closure(
                Box::new(Type::String),
                Box::new(Type::Optional(Box::new(Type::I64)))
            )
        ));

        // Function contravariance on parameters
        // Map(EqHash, I64) <: Closure(String, Optional<I64>)
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::EqHash), Box::new(Type::I64)),
            &Type::Closure(
                Box::new(Type::String),
                Box::new(Type::Optional(Box::new(Type::I64)))
            )
        ));

        // Function covariance on return type
        // Map(String, I64) <: Closure(String, Optional<Universe>)
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Closure(
                Box::new(Type::String),
                Box::new(Type::Optional(Box::new(Type::Universe))) // Supertype of I64 (covariance)
            )
        ));

        // Negative test - parameter type doesn't match
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::I64)),
            &Type::Closure(
                Box::new(Type::I64), // String is not a subtype of I64
                Box::new(Type::Optional(Box::new(Type::I64)))
            )
        ));

        // Test Array as a subtype of Function
        // Array(String) <: Closure(I64, Optional<String>)
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::String)),
            &Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::Optional(Box::new(Type::String)))
            )
        ));

        // Function covariance on return type
        // Array(String) <: Closure(I64, Optional<Universe>)
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::String)),
            &Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::Optional(Box::new(Type::Universe))) // Supertype of String (covariance)
            )
        ));

        // Negative test - parameter type must be I64
        assert!(!registry.is_subtype(
            &Type::Array(Box::new(Type::String)),
            &Type::Closure(
                Box::new(Type::String), // Must be I64
                Box::new(Type::Optional(Box::new(Type::String)))
            )
        ));

        // Negative test - non-optional return type not compatible
        assert!(!registry.is_subtype(
            &Type::Array(Box::new(Type::String)),
            &Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::String) // Not Optional<String>
            )
        ));

        // Tuples are not subtypes of functions
        assert!(!registry.is_subtype(
            &Type::Tuple(vec![Type::String, Type::String]),
            &Type::Closure(
                Box::new(Type::I64),
                Box::new(Type::Optional(Box::new(Type::String)))
            )
        ));
    }
}
