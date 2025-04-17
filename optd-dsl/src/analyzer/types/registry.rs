use crate::analyzer::errors::AnalyzerErrorKind;
use crate::analyzer::hir::{Identifier, TypedSpan};
use crate::parser::ast::{Adt, Field, Type as AstType};
use crate::utils::span::Span;
use Adt::*;
use std::collections::BTreeMap;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

// Core type constants.
// These are expressed as strings, not a Type enum, because they are
// regular ADTs in the language.
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
    I64,
    String,
    Bool,
    F64,

    // Special types.
    Unit,
    Universe,       // All types are subtypes of Universe.
    Nothing,        // Inherits all types.
    None,           // Inherits all optionals.
    Unknown(usize), // Unique identifier for unknown types.

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

    // Native trait types
    Concat,     // For types that can be concatenated (String, Array, Map).
    EqHash,     // For types that support equality and hashing.
    Arithmetic, // For types that support arithmetic operations.
}

/// Represents a constraint on a type.
///
/// Type constraints are used by the solver to determine the concrete types
/// of expressions in the type checking phase. Each constraint includes the
/// source code location where it was generated to provide useful error messages.
#[derive(Debug, Clone)]
pub enum Constraint {
    /// Subtyping relationship: `target_type >: sub_types`
    ///
    /// This constraint enforces that `target_type` is a supertype of all `sub_types`,
    /// allowing substitution of more specific types where a more general type is expected.
    Subtypes {
        target_type: TypedSpan,
        sub_types: Vec<TypedSpan>,
    },

    /// Equality constraint: `target_type = other_type`
    ///
    /// This constraint enforces that `target_type` is equal to `other_type`.
    Equal {
        target_type: TypedSpan,
        other_type: TypedSpan,
    },

    /// Field access: `inner.field = outer`
    ///
    /// This constraint enforces that the type `inner` has a field named `field`
    /// with a type that matches `outer`. Used for struct field access.
    FieldAccess {
        inner: TypedSpan,
        field: Identifier,
        outer: TypedSpan,
    },
}

/// Manages the type hierarchy, subtyping relationships, and constraint solving
///
/// The TypeRegistry keeps track of the inheritance relationships between
/// types, particularly for user-defined ADTs. It provides methods to register
/// types, check subtyping relationships, and solve type constraints.
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
    /// The type of the return value of the current function.
    pub ty_return: Option<TypedSpan>,
    /// The set of collected type constraints to be solved.
    pub constraints: Vec<Constraint>,
    /// Maps unknown type IDs to their resolved concrete types.
    pub resolved_unknown: HashMap<usize, Type>,
    /// Current ID to use for new Unknown types
    pub next_unknown_id: usize,
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
    /// `Ok(())` if registration is successful, or a `AnalyzerErrorKind` if a duplicate name is found.
    pub fn register_adt(&mut self, adt: &Adt) -> Result<(), Box<AnalyzerErrorKind>> {
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

    /// Creates a new TypeRegistry instance
    pub fn new() -> Self {
        Self {
            subtypes: BTreeMap::new(),
            spans: HashMap::new(),
            product_fields: HashMap::new(),
            ty_return: None,
            constraints: Vec::new(),
            resolved_unknown: HashMap::new(),
            next_unknown_id: 0,
        }
    }

    // The is_subtype method is now provided in the subtypes.rs file

    /// Retrieves the type of a field from a product ADT by name.
    ///
    /// This function should only be called once the registry has been validated,
    /// or it might panic.
    pub fn get_product_field_type(&self, adt_name: &Identifier, field_name: &Identifier) -> Type {
        let fields = self
            .product_fields
            .get(adt_name)
            .unwrap_or_else(|| panic!("ADT '{}' not found in type registry", adt_name));

        let field = fields
            .iter()
            .find(|field| *field.name.value == *field_name)
            .cloned()
            .unwrap_or_else(|| panic!("Field '{}' not found in ADT '{}'", field_name, adt_name));

        convert_ast_type(*field.ty.value)
    }

    /// Retrieves the type of a field from a product ADT by index position.
    ///
    /// This function should only be called once the registry has been validated,
    /// or it might panic.
    pub fn get_product_field_type_by_index(&self, adt_name: &Identifier, index: usize) -> Type {
        let fields = self
            .product_fields
            .get(adt_name)
            .unwrap_or_else(|| panic!("ADT '{}' not found in type registry", adt_name));

        let field = &fields[index];
        convert_ast_type(*field.ty.value.clone())
    }

    /// Creates a new unknown type
    pub fn new_unknown(&mut self) -> Type {
        let id = self.next_unknown_id;
        self.next_unknown_id += 1;
        Type::Unknown(id)
    }

    /// Adds a subtyping constraint set: `target_type >: all sub_types`
    ///
    /// This constraint enforces that the target_type is a supertype of all the sub_types.
    ///
    /// # Arguments
    ///
    /// * `target_type` - The type that must be a supertype of all sub_types
    /// * `sub_types` - The types that must be subtypes of target_type
    pub fn add_constraint_subtypes(&mut self, target_type: &TypedSpan, sub_types: &[TypedSpan]) {
        self.constraints.push(Constraint::Subtypes {
            target_type: target_type.clone(),
            sub_types: sub_types.to_vec(),
        });
    }

    /// Adds an equality constraint: `target_type = other_type`
    ///
    /// This constraint enforces that the target_type is equal to other_type.
    ///
    /// # Arguments
    ///
    /// * `target_type` - The type that is expected to be equal to other_type
    /// * `other_type` - The type that is being compared for equality
    pub fn add_constraint_equal(&mut self, target_type: &TypedSpan, other_type: &TypedSpan) {
        self.constraints.push(Constraint::Equal {
            target_type: target_type.clone(),
            other_type: other_type.clone(),
        });
    }

    /// Adds a field access constraint: `outer >: inner.field`
    ///
    /// This enforces that a type has a field with a particular name and type.
    ///
    /// # Arguments
    ///
    /// * `outer` - The expected type of the field
    /// * `field` - The name of the field with its source location
    /// * `inner` - The type containing the field
    pub fn add_constraint_field_access(
        &mut self,
        outer: &TypedSpan,
        field: &str,
        inner: &TypedSpan,
    ) {
        self.constraints.push(Constraint::FieldAccess {
            inner: inner.clone(),
            field: field.to_string(),
            outer: outer.clone(),
        });
    }
}

/// Creates a function type from parameter types and return type.
pub fn create_function_type(param_types: &[Type], return_type: &Type) -> Type {
    use Type::*;

    let param_type = if param_types.is_empty() {
        Unit
    } else if param_types.len() == 1 {
        param_types[0].clone()
    } else {
        Tuple(param_types.to_vec())
    };

    Closure(param_type.into(), return_type.clone().into())
}

/// Converts an AST type to a Type enum.
fn convert_ast_type(ty: AstType) -> Type {
    use Type::*;

    match ty {
        AstType::Identifier(name) => Adt(name),
        AstType::Int64 => I64,
        AstType::String => String,
        AstType::Bool => Bool,
        AstType::Unit => Unit,
        AstType::Float64 => F64,
        AstType::Array(inner) => Array(convert_ast_type(*inner.value).into()),
        AstType::Closure(params, ret) => Closure(
            convert_ast_type(*params.value).into(),
            convert_ast_type(*ret.value).into(),
        ),
        AstType::Tuple(inner) => {
            let inner_types = inner
                .iter()
                .map(|ty| convert_ast_type(*ty.value.clone()))
                .collect();
            Tuple(inner_types)
        }
        AstType::Map(key, val) => Map(
            convert_ast_type(*key.value).into(),
            convert_ast_type(*val.value).into(),
        ),
        AstType::Questioned(inner) => Optional(convert_ast_type(*inner.value).into()),
        _ => panic!("Registry has not been properly validated"),
    }
}
#[cfg(test)]
pub mod type_registry_tests {
    use super::*;
    use crate::{
        parser::ast::Field,
        utils::span::{Span, Spanned},
    };

    pub fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    pub fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    pub fn create_product_adt(name: &str, fields: Vec<(&str, AstType)>) -> Adt {
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

    pub fn create_sum_adt(name: &str, variants: Vec<Adt>) -> Adt {
        Sum {
            name: spanned(name.to_string()),
            variants: variants.into_iter().map(spanned).collect(),
        }
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
    fn test_create_function_type() {
        // Test with no parameters
        let params: Vec<Type> = vec![];
        let return_type = Type::I64;
        let result = create_function_type(&params, &return_type);
        match result {
            Type::Closure(param, ret) => {
                assert_eq!(*param, Type::Unit);
                assert_eq!(*ret, Type::I64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with one parameter
        let params = vec![Type::I64];
        let result = create_function_type(&params, &return_type);
        match result {
            Type::Closure(param, ret) => {
                assert_eq!(*param, Type::I64);
                assert_eq!(*ret, Type::I64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with multiple parameters
        let params = vec![Type::I64, Type::Bool];
        let result = create_function_type(&params, &return_type);
        match result {
            Type::Closure(param, ret) => {
                match &*param {
                    Type::Tuple(types) => {
                        assert_eq!(types.len(), 2);
                        assert_eq!(types[0], Type::I64);
                        assert_eq!(types[1], Type::Bool);
                    }
                    _ => panic!("Expected Tuple type for parameters"),
                }
                assert_eq!(*ret, Type::I64);
            }
            _ => panic!("Expected Closure type"),
        }
    }
}
