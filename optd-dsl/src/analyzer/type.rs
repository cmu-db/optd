use crate::parser::ast::Adt;
use std::collections::{HashMap, HashSet};
pub type Identifier = String;

/// Represents types in the language
///
/// This enum contains both primitive types (like Int64, String) and complex types
/// (like Array, Tuple, Closure) as well as user-defined types through ADTs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    // Primitive types
    Int64,
    String,
    Bool,
    Float64,
    Unit,
    // Complex types
    Array(Box<Type>),
    Closure(Box<Type>, Box<Type>),
    Tuple(Vec<Type>),
    Map(Box<Type>, Box<Type>),
    // User-defined types
    Adt(Identifier),
}

/// A typed value that carries both a value and its type information.
///
/// This generic wrapper allows attaching type information to any value in the compiler pipeline.
/// It's particularly useful for tracking types through expressions, statements, and other AST nodes
/// during the type checking phase.
///
/// # Type Parameters
///
/// * `T` - The type of the wrapped value
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Typed<T> {
    /// The wrapped value
    pub value: Box<T>,

    /// The type of the value
    pub ty: Type,
}

impl<T> Typed<T> {
    /// Creates a new typed value with the given value and type.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to wrap
    /// * `ty` - The type to associate with the value
    ///
    /// # Returns
    ///
    /// A new `Typed<T>` instance containing the value and its type
    pub fn new(value: T, ty: Type) -> Self {
        Self {
            value: Box::new(value),
            ty,
        }
    }

    /// Checks if this typed value's type is a subtype of another typed value's type.
    ///
    /// This is useful for type checking to ensure type compatibility between
    /// expressions, assignments, function calls, etc.
    ///
    /// # Arguments
    ///
    /// * `other` - The other typed value to compare against
    /// * `registry` - The type registry that defines subtyping relationships
    ///
    /// # Returns
    ///
    /// `true` if this type is a subtype of the other, `false` otherwise
    pub fn subtype_of(&self, other: &Self, registry: &TypeRegistry) -> bool {
        registry.is_subtype(&self.ty, &other.ty)
    }
}

/// Manages the type hierarchy and subtyping relationships
///
/// The TypeRegistry keeps track of the inheritance relationships between
/// types, particularly for user-defined ADTs. It provides methods to register
/// types and check subtyping relationships.
#[derive(Debug)]
pub struct TypeRegistry {
    subtypes: HashMap<Identifier, HashSet<Identifier>>,
}

impl TypeRegistry {
    /// Creates a new empty TypeRegistry
    pub fn new() -> Self {
        TypeRegistry {
            subtypes: HashMap::new(),
        }
    }

    /// Registers an ADT in the type registry
    ///
    /// This method updates the type hierarchy by adding the ADT and all its
    /// potential subtypes to the registry. For enums, each variant is registered
    /// as a subtype of the enum.
    ///
    /// # Arguments
    ///
    /// * `adt` - The ADT to register
    pub fn register_adt(&mut self, adt: &Adt) {
        match adt {
            Adt::Product { name, .. } => {
                let type_name = *name.value.clone();
                self.subtypes.entry(type_name).or_default();
            }
            Adt::Sum { name, variants } => {
                let enum_name = *name.value.clone();
                self.subtypes.entry(enum_name.clone()).or_default();
                for variant in variants {
                    let variant_adt = variant.value.as_ref();
                    self.register_adt(&variant_adt);
                    let variant_name = match variant_adt {
                        Adt::Product { name, .. } => name.value.clone(),
                        Adt::Sum { name, .. } => name.value.clone(),
                    };
                    if let Some(children) = self.subtypes.get_mut(&enum_name) {
                        children.insert(*variant_name);
                    }
                }
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
        if child == parent {
            return true;
        }

        match (child, parent) {
            // Check transitive inheritance
            (Type::Adt(child_name), Type::Adt(parent_name)) => {
                if child_name == parent_name {
                    return true;
                }

                self.subtypes.get(parent_name).map_or(false, |children| {
                    children.iter().any(|subtype_child_name| {
                        self.is_subtype(
                            &Type::Adt(child_name.clone()),
                            &Type::Adt(subtype_child_name.clone()),
                        )
                    })
                })
            }
            // Array covariance: Array[T] <: Array[U] if T <: U
            (Type::Array(child_elem), Type::Array(parent_elem)) => {
                self.is_subtype(child_elem, parent_elem)
            }
            // Tuple covariance: (T1, T2, ...) <: (U1, U2, ...) if T1 <: U1, T2 <: U2, ...
            (Type::Tuple(child_types), Type::Tuple(parent_types)) => {
                if child_types.len() != parent_types.len() {
                    return false;
                }
                child_types
                    .iter()
                    .zip(parent_types.iter())
                    .all(|(c, p)| self.is_subtype(c, p))
            }
            // Map covariance: Map[K1, V1] <: Map[K2, V2] if K1 <: K2 and V1 <: V2
            (Type::Map(child_key, child_val), Type::Map(parent_key, parent_val)) => {
                self.is_subtype(child_key, parent_key) && self.is_subtype(child_val, parent_val)
            }
            // Function contravariance on args, covariance on return type:
            // (T1 -> U1) <: (T2 -> U2) if T2 <: T1 and U1 <: U2
            (Type::Closure(child_param, child_ret), Type::Closure(parent_param, parent_ret)) => {
                self.is_subtype(parent_param, child_param) && self.is_subtype(child_ret, parent_ret)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod type_registry_tests {
    use crate::{
        errors::span::{Span, Spanned},
        parser::ast::{self, Field},
    };

    use super::*;

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

        Adt::Product {
            name: spanned(name.to_string()),
            fields: spanned_fields,
        }
    }

    fn create_sum_adt(name: &str, variants: Vec<Adt>) -> Adt {
        let spanned_variants: Vec<Spanned<Adt>> = variants
            .into_iter()
            .map(|variant| spanned(variant))
            .collect();

        Adt::Sum {
            name: spanned(name.to_string()),
            variants: spanned_variants,
        }
    }

    #[test]
    fn test_primitive_type_equality() {
        let registry = TypeRegistry::new();

        // Same primitive types should be subtypes of each other
        assert!(registry.is_subtype(&Type::Int64, &Type::Int64));
        assert!(registry.is_subtype(&Type::Bool, &Type::Bool));
        assert!(registry.is_subtype(&Type::String, &Type::String));
        assert!(registry.is_subtype(&Type::Float64, &Type::Float64));
        assert!(registry.is_subtype(&Type::Unit, &Type::Unit));

        // Different primitive types should not be subtypes
        assert!(!registry.is_subtype(&Type::Int64, &Type::Bool));
        assert!(!registry.is_subtype(&Type::String, &Type::Int64));
        assert!(!registry.is_subtype(&Type::Float64, &Type::Int64));
        assert!(!registry.is_subtype(&Type::Unit, &Type::Bool));
    }

    #[test]
    fn test_array_subtyping() {
        let registry = TypeRegistry::new();

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

        // Array with inheritance (will be tested more with ADTs)
        let mut adts_registry = TypeRegistry::new();
        let vehicle = create_product_adt("Vehicle", vec![]);
        let car = create_product_adt("Car", vec![]);
        let vehicles_enum = create_sum_adt("Vehicles", vec![vehicle, car]);
        adts_registry.register_adt(&vehicles_enum);

        assert!(adts_registry.is_subtype(
            &Type::Array(Box::new(Type::Adt("Car".to_string()))),
            &Type::Array(Box::new(Type::Adt("Vehicles".to_string())))
        ));
    }

    #[test]
    fn test_tuple_subtyping() {
        let registry = TypeRegistry::new();

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
        let registry = TypeRegistry::new();

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
    }

    #[test]
    fn test_closure_subtyping() {
        let registry = TypeRegistry::new();

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

        // Contravariant parameter types - broader param type is a subtype
        let mut adts_registry = TypeRegistry::new();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum);

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
        let mut registry = TypeRegistry::new();

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
        registry.register_adt(&shapes_enum);

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
    fn test_complex_nested_type_hierarchy() {
        let mut registry = TypeRegistry::new();

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
        registry.register_adt(&vehicles_enum);

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
    fn test_combined_complex_types() {
        let mut registry = TypeRegistry::new();

        // Create and register a type hierarchy
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let cat = create_product_adt("Cat", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog, cat]);
        registry.register_adt(&animals_enum);

        // Test array of ADTs
        assert!(registry.is_subtype(
            &Type::Array(Box::new(Type::Adt("Dog".to_string()))),
            &Type::Array(Box::new(Type::Adt("Animals".to_string())))
        ));

        // Test tuple of ADTs
        assert!(registry.is_subtype(
            &Type::Tuple(vec![
                Type::Adt("Dog".to_string()),
                Type::Adt("Cat".to_string())
            ]),
            &Type::Tuple(vec![
                Type::Adt("Animals".to_string()),
                Type::Adt("Animals".to_string())
            ])
        ));

        // Test map with ADT keys and values
        assert!(registry.is_subtype(
            &Type::Map(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Cat".to_string()))
            ),
            &Type::Map(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::Adt("Animals".to_string()))
            )
        ));

        // Test closures with ADTs
        assert!(registry.is_subtype(
            &Type::Closure(
                Box::new(Type::Adt("Animals".to_string())),
                Box::new(Type::Adt("Dog".to_string()))
            ),
            &Type::Closure(
                Box::new(Type::Adt("Dog".to_string())),
                Box::new(Type::Adt("Animals".to_string()))
            )
        ));

        // Test deeply nested types
        assert!(registry.is_subtype(
            &Type::Map(
                Box::new(Type::String),
                Box::new(Type::Array(Box::new(Type::Tuple(vec![
                    Type::Adt("Dog".to_string()),
                    Type::Closure(
                        Box::new(Type::Adt("Animals".to_string())),
                        Box::new(Type::Adt("Cat".to_string()))
                    )
                ]))))
            ),
            &Type::Map(
                Box::new(Type::String),
                Box::new(Type::Array(Box::new(Type::Tuple(vec![
                    Type::Adt("Animals".to_string()),
                    Type::Closure(
                        Box::new(Type::Adt("Dog".to_string())),
                        Box::new(Type::Adt("Animals".to_string()))
                    )
                ]))))
            )
        ));
    }
}
