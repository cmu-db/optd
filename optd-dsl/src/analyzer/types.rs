use super::{error::AnalyzerErrorKind, hir::Identifier};
use crate::parser::ast::{Adt, Field};
use crate::utils::span::Span;
use Adt::*;
use std::collections::BTreeMap;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

// Core type constants.
pub const LOGICAL_TYPE: &str = "Logical";
pub const PHYSICAL_TYPE: &str = "Physical";
pub const LOGICAL_PROPS: &str = "LogicalProperties";
pub const PHYSICAL_PROPS: &str = "PhysicalProperties";

pub const CORE_TYPES: [&str; 4] = [LOGICAL_TYPE, PHYSICAL_TYPE, LOGICAL_PROPS, PHYSICAL_PROPS];

/// Represents types in the language.
///
/// This enum contains both primitive types (like Int64, String) and complex types
/// (like Array, Tuple, Closure) as well as user-defined types through ADTs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    // Primitive types.
    Int64,
    String,
    Bool,
    Float64,

    // Special types.
    Unit,
    Universe, // All types are subtypes of Universe.
    Never,    // Inherits all types.
    Unknown,

    // User types.
    Adt(Identifier),
    Generic(Identifier),

    // Composite types.
    Array(Box<Type>),
    Closure(Box<Type>, Box<Type>),
    Tuple(Vec<Type>),
    Map(Box<Type>, Box<Type>),
    Optional(Box<Type>),

    // For Logical & Physical: memo status.
    Stored(Box<Type>),
    Costed(Box<Type>),
}

/// Manages the type hierarchy and subtyping relationships
///
/// The TypeRegistry keeps track of the inheritance relationships between
/// types, particularly for user-defined ADTs. It provides methods to register
/// types and check subtyping relationships.
#[derive(Debug, Clone, Default)]
pub struct TypeRegistry {
    /// Maps ADT identifiers to their subtype identifiers.
    /// We use a BTreeMap to ensure determinstic execution.
    pub subtypes: BTreeMap<Identifier, HashSet<Identifier>>,
    /// Maps ADT identifiers to their source spans.
    pub spans: HashMap<Identifier, Span>,
    /// Maps terminal / product ADT identifiers to their AST fields.
    ///
    /// Note: We store the original AST Field elements rather than converting them
    /// to a more processed form to preserve their source span information. This allows
    /// for better error reporting during type checking and semantic analysis.
    pub product_fields: HashMap<Identifier, Vec<Field>>,
}

impl TypeRegistry {
    /// Registers an ADT in the type registry
    ///
    /// This method updates the type hierarchy by adding the ADT and all its
    /// potential subtypes to the registry. For enums, each variant is registered
    /// as a subtype of the enum.
    ///
    /// # Arguments
    ///
    /// * `adt` - The ADT to register
    ///
    /// # Returns
    ///
    /// `Ok(())` if registration is successful, or a `AnalyzerErrorKind` if a duplicate name is found    
    pub fn register_adt(&mut self, adt: &Adt) -> Result<(), AnalyzerErrorKind> {
        match adt {
            Product { name, fields } => {
                let type_name = name.value.as_ref().clone();

                // Check for duplicate ADT names.
                if let Some(existing_span) = self.spans.get(&type_name) {
                    return Err(AnalyzerErrorKind::new_duplicate_adt(
                        &type_name,
                        existing_span,
                        &name.span,
                    ));
                }

                // Register the ADT fields.
                self.product_fields.insert(
                    type_name.clone(),
                    fields.iter().map(|field| *field.value.clone()).collect(),
                );

                self.spans.insert(type_name.clone(), name.span.clone());
                self.subtypes.entry(type_name).or_default();

                Ok(())
            }
            Sum { name, variants } => {
                let enum_name = name.value.as_ref().clone();

                // Check for duplicate ADT names.
                if let Some(existing_span) = self.spans.get(&enum_name) {
                    return Err(AnalyzerErrorKind::new_duplicate_adt(
                        &enum_name,
                        existing_span,
                        &name.span,
                    ));
                }

                self.spans.insert(enum_name.clone(), name.clone().span);
                self.subtypes.entry(enum_name.clone()).or_default();

                for variant in variants {
                    let variant_adt = variant.value.as_ref();
                    // Register each variant.
                    self.register_adt(variant_adt)?;

                    let variant_name = match variant_adt {
                        Product { name, .. } => name.value.as_ref(),
                        Sum { name, .. } => name.value.as_ref(),
                    };

                    // Add variant as a subtype of the enum.
                    if let Some(children) = self.subtypes.get_mut(&enum_name) {
                        children.insert(variant_name.clone());
                    }
                }
                Ok(())
            }
        }
    }

    /// Checks if a type is a subtype of another type
    ///
    /// This method determines if `child` is a subtype of `parent` according to
    /// the language's type system rules. It handles primitive types, complex types
    /// with covariant and contravariant relationships, and user-defined ADTs.
    ///
    /// # Arguments
    ///
    /// * `child` - The potential subtype
    /// * `parent` - The potential supertype
    ///
    /// # Returns
    ///
    /// `true` if `child` is a subtype of `parent`, `false` otherwise
    pub fn is_subtype(&self, child: &Type, parent: &Type) -> bool {
        use Type::*;

        if child == parent {
            return true;
        }

        match (child, parent) {
            // Universe is the top type - everything is a subtype of Universe
            (_, Universe) => true,

            // Never is the bottom type - it is a subtype of everything
            (Never, _) => true,

            // Stored and Costed type handling
            (Stored(child_inner), Stored(parent_inner)) => {
                self.is_subtype(child_inner, parent_inner)
            }
            (Costed(child_inner), Costed(parent_inner)) => {
                self.is_subtype(child_inner, parent_inner)
            }
            (Costed(child_inner), Stored(parent_inner)) => {
                // Costed(A) is a subtype of Stored(A).
                self.is_subtype(child_inner, parent_inner)
            }
            (Costed(child_inner), parent_inner) => {
                // Costed(A) is a subtype of A.
                self.is_subtype(child_inner, parent_inner)
            }
            (Stored(child_inner), parent_inner) => {
                // Stored(A) is a subtype of A.
                self.is_subtype(child_inner, parent_inner)
            }

            // Check transitive inheritance for ADTs
            (Adt(child_name), Adt(parent_name)) => {
                if child_name == parent_name {
                    return true;
                }

                self.subtypes.get(parent_name).is_some_and(|children| {
                    children.iter().any(|subtype_child_name| {
                        self.is_subtype(&Adt(child_name.clone()), &Adt(subtype_child_name.clone()))
                    })
                })
            }

            // Array covariance: Array[T] <: Array[U] if T <: U
            (Array(child_elem), Array(parent_elem)) => self.is_subtype(child_elem, parent_elem),

            // Tuple covariance: (T1, T2, ...) <: (U1, U2, ...) if T1 <: U1, T2 <: U2, ...
            (Tuple(child_types), Tuple(parent_types)) => {
                if child_types.len() != parent_types.len() {
                    return false;
                }
                child_types
                    .iter()
                    .zip(parent_types.iter())
                    .all(|(c, p)| self.is_subtype(c, p))
            }

            // Map covariance: Map[K1, V1] <: Map[K2, V2] if K1 <: K2 and V1 <: V2
            (Map(child_key, child_val), Map(parent_key, parent_val)) => {
                self.is_subtype(child_key, parent_key) && self.is_subtype(child_val, parent_val)
            }

            // Function contravariance on args, covariance on return type:
            // (T1 -> U1) <: (T2 -> U2) if T2 <: T1 and U1 <: U2
            (Closure(child_param, child_ret), Closure(parent_param, parent_ret)) => {
                self.is_subtype(parent_param, child_param) && self.is_subtype(child_ret, parent_ret)
            }

            // Optional type covariance: Optional[T] <: Optional[U] if T <: U
            (Optional(child_ty), Optional(parent_ty)) => self.is_subtype(child_ty, parent_ty),

            _ => false,
        }
    }
}

#[cfg(test)]
mod type_registry_tests {
    use super::*;
    use crate::{
        parser::ast::{self, Field},
        utils::span::{Span, Spanned},
    };

    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    fn create_product_adt(name: &str, fields: Vec<(&str, ast::Type)>) -> Adt {
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
    fn test_stored_and_costed_types() {
        let registry = TypeRegistry::default();

        // Test Stored type as a subtype of the inner type
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::Int64)), &Type::Int64));

        // Test Costed type as a subtype of Stored type
        assert!(registry.is_subtype(
            &Type::Costed(Box::new(Type::Int64)),
            &Type::Stored(Box::new(Type::Int64))
        ));

        // Test Costed type as a subtype of the inner type (transitivity)
        assert!(registry.is_subtype(&Type::Costed(Box::new(Type::Int64)), &Type::Int64));

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
    fn test_duplicate_adt_detection() {
        let mut registry = TypeRegistry::default();

        // First registration should succeed
        let car1 = create_product_adt("Car", vec![]);
        assert!(registry.register_adt(&car1).is_ok());

        // Second registration with the same name should fail
        let car2 = Product {
            name: spanned("Car".to_string()),
            fields: vec![],
        };

        let result = registry.register_adt(&car2);
        assert!(result.is_err());
    }

    #[test]
    fn test_primitive_type_equality() {
        let registry = TypeRegistry::default();

        // Same primitive types should be subtypes of each other
        assert!(registry.is_subtype(&Type::Int64, &Type::Int64));
        assert!(registry.is_subtype(&Type::Bool, &Type::Bool));
        assert!(registry.is_subtype(&Type::String, &Type::String));
        assert!(registry.is_subtype(&Type::Float64, &Type::Float64));
        assert!(registry.is_subtype(&Type::Unit, &Type::Unit));
        assert!(registry.is_subtype(&Type::Universe, &Type::Universe));

        // Different primitive types should not be subtypes
        assert!(!registry.is_subtype(&Type::Int64, &Type::Bool));
        assert!(!registry.is_subtype(&Type::String, &Type::Int64));
        assert!(!registry.is_subtype(&Type::Float64, &Type::Int64));
        assert!(!registry.is_subtype(&Type::Unit, &Type::Bool));

        // All types should be subtypes of Universe
        assert!(registry.is_subtype(&Type::Int64, &Type::Universe));
        assert!(registry.is_subtype(&Type::Bool, &Type::Universe));
        assert!(registry.is_subtype(&Type::String, &Type::Universe));
        assert!(registry.is_subtype(&Type::Float64, &Type::Universe));
        assert!(registry.is_subtype(&Type::Unit, &Type::Universe));
    }

    #[test]
    fn test_array_subtyping() {
        let registry = TypeRegistry::default();

        // Same type arrays
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::Int64)),
            &Type::Array(Box::new(Type::Int64))
        ));

        // Different type arrays
        assert!(!registry.is_subtype(
            &Type::Array(Box::new(Type::Int64)),
            &Type::Array(Box::new(Type::Bool))
        ));

        // Test nested arrays
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::Array(Box::new(Type::Int64)))),
            &Type::Array(Box::new(Type::Array(Box::new(Type::Int64))))
        ));

        // Array of any type is subtype of Universe
        assert!(registry.is_subtype(&Type::Array(Box::new(Type::Int64)), &Type::Universe));

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
        let registry = TypeRegistry::default();

        // Same type tuples
        assert!(registry.is_subtype(
            &Type::Tuple(vec![Type::Int64, Type::Bool]),
            &Type::Tuple(vec![Type::Int64, Type::Bool])
        ));

        // Different type tuples
        assert!(!registry.is_subtype(
            &Type::Tuple(vec![Type::Int64, Type::Bool]),
            &Type::Tuple(vec![Type::Bool, Type::Int64])
        ));

        // Different length tuples
        assert!(!registry.is_subtype(
            &Type::Tuple(vec![Type::Int64, Type::Bool]),
            &Type::Tuple(vec![Type::Int64, Type::Bool, Type::String])
        ));

        // Empty tuples
        assert!(registry.is_subtype(&Type::Tuple(vec![]), &Type::Tuple(vec![])));

        // All tuples are subtypes of Universe
        assert!(registry.is_subtype(&Type::Tuple(vec![Type::Int64, Type::Bool]), &Type::Universe));

        // Nested tuples
        assert!(registry.is_subtype(
            &Type::Tuple(vec![
                Type::Int64,
                Type::Tuple(vec![Type::Bool, Type::String])
            ]),
            &Type::Tuple(vec![
                Type::Int64,
                Type::Tuple(vec![Type::Bool, Type::String])
            ])
        ));
    }

    #[test]
    fn test_map_subtyping() {
        let registry = TypeRegistry::default();

        // Same type maps
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::Int64)),
            &Type::Map(Box::new(Type::String), Box::new(Type::Int64))
        ));

        // Different key types
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::Int64)),
            &Type::Map(Box::new(Type::Int64), Box::new(Type::Int64))
        ));

        // Different value types
        assert!(!registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::Int64)),
            &Type::Map(Box::new(Type::String), Box::new(Type::Bool))
        ));

        // All maps are subtypes of Universe
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::Int64)),
            &Type::Universe
        ));
    }

    #[test]
    fn test_closure_subtyping() {
        let registry = TypeRegistry::default();

        // Same function signatures
        assert!(registry.is_subtype(
            &Type::Closure(Box::new(Type::Int64), Box::new(Type::Bool)),
            &Type::Closure(Box::new(Type::Int64), Box::new(Type::Bool))
        ));

        // Contravariant parameter types - narrower param type is not a subtype
        assert!(!registry.is_subtype(
            &Type::Closure(Box::new(Type::Int64), Box::new(Type::Bool)),
            &Type::Closure(Box::new(Type::Float64), Box::new(Type::Bool))
        ));

        // All closures are subtypes of Universe
        assert!(registry.is_subtype(
            &Type::Closure(Box::new(Type::Int64), Box::new(Type::Bool)),
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
            &Type::Closure(
                Box::new(Type::Int64),
                Box::new(Type::Adt("Dog".to_string()))
            ),
            &Type::Closure(
                Box::new(Type::Int64),
                Box::new(Type::Adt("Animals".to_string()))
            )
        ));
    }

    #[test]
    fn test_adt_registration_and_subtyping() {
        let mut registry = TypeRegistry::default();

        // Create a simple Shape hierarchy
        let shape = create_product_adt("Shape", vec![("area", ast::Type::Float64)]);
        let circle = create_product_adt(
            "Circle",
            vec![("radius", ast::Type::Float64), ("area", ast::Type::Float64)],
        );
        let rectangle = create_product_adt(
            "Rectangle",
            vec![
                ("width", ast::Type::Float64),
                ("height", ast::Type::Float64),
                ("area", ast::Type::Float64),
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
        let registry = TypeRegistry::default();

        // Check that Universe is a supertype of all primitive types
        assert!(registry.is_subtype(&Type::Int64, &Type::Universe));
        assert!(registry.is_subtype(&Type::String, &Type::Universe));
        assert!(registry.is_subtype(&Type::Bool, &Type::Universe));
        assert!(registry.is_subtype(&Type::Float64, &Type::Universe));
        assert!(registry.is_subtype(&Type::Unit, &Type::Universe));

        // Check that Universe is a supertype of all complex types
        assert!(registry.is_subtype(&Type::Array(Box::new(Type::Int64)), &Type::Universe));
        assert!(registry.is_subtype(&Type::Tuple(vec![Type::Int64, Type::Bool]), &Type::Universe));
        assert!(registry.is_subtype(
            &Type::Map(Box::new(Type::String), Box::new(Type::Int64)),
            &Type::Universe
        ));
        assert!(registry.is_subtype(
            &Type::Closure(Box::new(Type::Int64), Box::new(Type::Bool)),
            &Type::Universe
        ));

        // Check that Universe is a supertype of Stored and Costed types
        assert!(registry.is_subtype(&Type::Stored(Box::new(Type::Int64)), &Type::Universe));
        assert!(registry.is_subtype(&Type::Costed(Box::new(Type::Int64)), &Type::Universe));

        // But Universe is not a subtype of any other type
        assert!(!registry.is_subtype(&Type::Universe, &Type::Int64));
        assert!(!registry.is_subtype(&Type::Universe, &Type::String));
        assert!(!registry.is_subtype(&Type::Universe, &Type::Array(Box::new(Type::Int64))));
    }

    #[test]
    fn test_never_as_bottom_type() {
        let registry = TypeRegistry::default();

        // Never is a subtype of all primitive types
        assert!(registry.is_subtype(&Type::Never, &Type::Int64));
        assert!(registry.is_subtype(&Type::Never, &Type::String));
        assert!(registry.is_subtype(&Type::Never, &Type::Bool));
        assert!(registry.is_subtype(&Type::Never, &Type::Float64));
        assert!(registry.is_subtype(&Type::Never, &Type::Unit));
        assert!(registry.is_subtype(&Type::Never, &Type::Universe));

        // Never is a subtype of complex types
        assert!(registry.is_subtype(&Type::Never, &Type::Array(Box::new(Type::Int64))));
        assert!(registry.is_subtype(&Type::Never, &Type::Tuple(vec![Type::Int64, Type::Bool])));
        assert!(registry.is_subtype(
            &Type::Never,
            &Type::Closure(Box::new(Type::Int64), Box::new(Type::Bool))
        ));

        // But no type is a subtype of Never (except Never itself)
        assert!(!registry.is_subtype(&Type::Int64, &Type::Never));
        assert!(!registry.is_subtype(&Type::Bool, &Type::Never));
        assert!(!registry.is_subtype(&Type::Universe, &Type::Never));
        assert!(!registry.is_subtype(&Type::Array(Box::new(Type::Int64)), &Type::Never));
    }

    #[test]
    fn test_complex_nested_type_hierarchy() {
        let mut registry = TypeRegistry::default();

        // Create a complex type hierarchy for vehicles
        let vehicle = create_product_adt("Vehicle", vec![("wheels", ast::Type::Int64)]);

        let car = create_product_adt(
            "Car",
            vec![("wheels", ast::Type::Int64), ("doors", ast::Type::Int64)],
        );

        let sports_car = create_product_adt(
            "SportsCar",
            vec![
                ("wheels", ast::Type::Int64),
                ("doors", ast::Type::Int64),
                ("top_speed", ast::Type::Float64),
            ],
        );

        let truck = create_product_adt(
            "Truck",
            vec![
                ("wheels", ast::Type::Int64),
                ("load_capacity", ast::Type::Float64),
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
}
