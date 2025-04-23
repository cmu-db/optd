use crate::dsl::analyzer::errors::AnalyzerErrorKind;
use crate::dsl::analyzer::hir::{Identifier, TypedSpan};
use crate::dsl::parser::ast::{Adt, Field, Type as AstType};
use crate::dsl::utils::span::{OptionalSpanned, Span, Spanned};
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

/// Represents the core structure of a type without metadata.
///
/// This enum contains both primitive types (like Int64, String) and complex types
/// (like Array, Tuple, Closure) as well as user-defined types through ADTs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeKind {
    // Primitive types.
    I64,
    String,
    Bool,
    F64,

    // Special types.
    Unit,
    Universe, // All types are subtypes of Universe.
    Nothing,  // Inherits all types.
    None,     // Inherits all optionals.

    // Unknown types.
    UnknownAsc(usize),  // Strictly ascending types.
    UnknownDesc(usize), // Strictly descending types.

    // User types.
    Adt(Identifier),
    Generic(Identifier),

    // Composite types.
    Array(Type),
    Closure(Type, Type),
    Tuple(Vec<Type>),
    Map(Type, Type),
    Optional(Type),

    // For Logical & Physical: memo status.
    Stored(Type),
    Costed(Type),

    // Native trait types
    Concat,     // For types that can be concatenated (String, Array, Map).
    EqHash,     // For types that support equality and hashing.
    Arithmetic, // For types that support arithmetic operations.
}

/// Represents a type, potentially with span information.
///
/// This struct wraps a TypeKind with optional span information, allowing
/// types to carry source location data when needed.
pub type Type = OptionalSpanned<TypeKind>;

impl From<TypeKind> for Type {
    fn from(kind: TypeKind) -> Self {
        OptionalSpanned::unspanned(kind)
    }
}

/// Represents a constraint on a type.
///
/// Type constraints are used by the solver to determine the concrete types
/// of expressions in the type checking phase. Each constraint includes the
/// source code location where it was generated to provide useful error messages.
#[derive(Debug, Clone)]
pub enum Constraint {
    /// Subtyping relationship: `parent >: child`
    ///
    /// This constraint enforces that `parent` is a supertype of `child`,
    /// allowing substitution of a more specific type where a more general type is expected.
    Subtype { parent: Type, child: TypedSpan },

    /// Closure call: `outer >: inner(args)`
    ///
    /// This constraint enforces that the function type `inner` can be called with
    /// the arguments `args`, and that the result type matches `outer`.
    Call {
        inner: TypedSpan,
        args: Vec<TypedSpan>,
        outer: TypedSpan,
    },

    /// Field access: `outer >: inner.field`
    ///
    /// This constraint enforces that the type `inner` has a field named `field`
    /// with a type that matches `outer`.
    FieldAccess {
        inner: TypedSpan,
        field: Identifier,
        outer: Type,
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
    pub ty_return: Option<Type>,
    /// The set of collected type constraints to be solved.
    pub constraints: Vec<Constraint>,
    /// Maps unknown type IDs to their current inferred concrete types.
    /// Types start at `Nothing`, and get bumped up when needed by the constraint.
    pub resolved_unknown: HashMap<usize, Type>,
    /// Current ID to use for new Unknown types.
    pub next_unknown_id: usize,
}

impl TypeRegistry {
    /// Creates a new TypeRegistry instance.
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

    /// Retrieves the type of a field from a product ADT by name.
    ///
    /// This function should only be called once the registry has been validated,
    /// or it might panic.
    pub fn get_product_field_type(
        &self,
        adt_name: &Identifier,
        field_name: &Identifier,
    ) -> Option<Type> {
        let fields = self.product_fields.get(adt_name)?;
        let field = fields
            .iter()
            .find(|field| *field.name.value == *field_name)
            .cloned()?;

        Some(convert_ast_type(field.ty))
    }

    /// Retrieves the type of a field from a product ADT by index position.
    pub fn get_product_field_type_by_index(
        &self,
        adt_name: &Identifier,
        index: usize,
    ) -> Option<Type> {
        let fields = self.product_fields.get(adt_name)?;
        let field = fields.get(index).cloned()?;

        Some(convert_ast_type(field.ty))
    }

    /// Creates a new unknown, ascending type.
    pub fn new_unknown_asc(&mut self) -> TypeKind {
        use TypeKind::*;

        let id = self.next_unknown_id;
        self.next_unknown_id += 1;
        self.resolved_unknown.insert(id, Nothing.into());
        UnknownAsc(id)
    }

    /// Creates a new unknown, descending type.
    pub fn new_unknown_desc(&mut self) -> TypeKind {
        use TypeKind::*;

        let id = self.next_unknown_id;
        self.next_unknown_id += 1;
        self.resolved_unknown.insert(id, Universe.into());
        UnknownDesc(id)
    }

    /// Adds a subtyping constraint set: `parent >: all children`
    ///
    /// This constraint enforces that the parent is a supertype of all the children.
    ///
    /// # Arguments
    ///
    /// * `parent` - The type that must be a supertype of all children
    /// * `children` - The types that must be children of parent
    pub(super) fn add_constraint_subtypes(&mut self, parent: &Type, children: &[TypedSpan]) {
        for child in children {
            self.constraints.push(Constraint::Subtype {
                parent: parent.clone(),
                child: child.clone(),
            });
        }
    }

    /// Adds an equality constraint: `target_type = other_type`
    ///
    /// This constraint enforces that the target_type is equal to other_type.
    ///
    /// # Arguments
    ///
    /// * `target_type` - The type that is expected to be equal to other_type
    /// * `other_type` - The type that is being compared for equality
    pub(super) fn add_constraint_equal(&mut self, target_type: &TypedSpan, other_type: &TypedSpan) {
        self.constraints.push(Constraint::Subtype {
            child: target_type.clone(),
            parent: other_type.ty.clone(),
        });

        self.constraints.push(Constraint::Subtype {
            child: other_type.clone(),
            parent: target_type.ty.clone(),
        });
    }

    /// Adds a function call constraint: `outer >: inner(args)`
    ///
    /// This enforces that the function type `inner` can be called with
    /// the arguments `args`, and that the result type matches `outer`.
    ///
    /// # Arguments
    ///
    /// * `outer` - The expected return type of the function call
    /// * `inner` - The function type being called
    /// * `args` - The arguments being passed to the function
    pub(super) fn add_constraint_call(
        &mut self,
        outer: &TypedSpan,
        inner: &TypedSpan,
        args: &[TypedSpan],
    ) {
        self.constraints.push(Constraint::Call {
            inner: inner.clone(),
            args: args.to_vec(),
            outer: outer.clone(),
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
    pub(super) fn add_constraint_field_access(
        &mut self,
        outer: &Type,
        field: &Identifier,
        inner: &TypedSpan,
    ) {
        self.constraints.push(Constraint::FieldAccess {
            inner: inner.clone(),
            field: field.clone(),
            outer: outer.clone(),
        });
    }

    /// Resolves any Unknown types to their concrete types.
    ///
    /// This method checks if a type is an Unknown variant and replaces it
    /// with its concrete inferred type from the registry. Other types are
    /// returned as-is.
    ///
    /// # Arguments
    ///
    /// * `ty` - The type to resolve
    ///
    /// # Returns
    ///
    /// The resolved Type if it was an Unknown type, otherwise the original Type
    pub(super) fn resolve_type(&self, ty: &Type) -> Type {
        use TypeKind::*;

        match &*ty.value {
            UnknownAsc(id) | UnknownDesc(id) => {
                if let Some(resolved) = self.resolved_unknown.get(id) {
                    resolved.clone()
                } else {
                    ty.clone()
                }
            }
            _ => ty.clone(),
        }
    }
}

/// Creates a function type from parameter types and return type.
pub(crate) fn create_function_type(param_types: &[Type], return_type: &Type) -> Type {
    use TypeKind::*;

    let param_type = if param_types.is_empty() {
        Unit.into()
    } else if param_types.len() == 1 {
        param_types[0].clone()
    } else {
        Tuple(param_types.to_vec()).into()
    };

    Closure(param_type, return_type.clone()).into()
}

/// Converts an AST type to a HIR Type.
fn convert_ast_type(ast_ty: Spanned<AstType>) -> Type {
    use TypeKind::*;

    let span = ast_ty.span;
    let kind = match *ast_ty.value {
        AstType::Identifier(name) => Adt(name),
        AstType::Int64 => I64,
        AstType::String => String,
        AstType::Bool => Bool,
        AstType::Unit => Unit,
        AstType::Float64 => F64,
        AstType::Array(inner) => Array(convert_ast_type(inner)),
        AstType::Closure(params, ret) => Closure(convert_ast_type(params), convert_ast_type(ret)),
        AstType::Tuple(inner) => {
            let inner_types = inner.into_iter().map(convert_ast_type).collect();
            Tuple(inner_types)
        }
        AstType::Map(key, val) => Map(convert_ast_type(key), convert_ast_type(val)),
        AstType::Questioned(inner) => Optional(convert_ast_type(inner)),
        _ => panic!("Registry has not been properly validated"),
    };

    OptionalSpanned::spanned(kind, span)
}

/// Converts a type to its string representation, resolving unknown types when possible.
///
/// This function formats a type as a string, including proper handling of unknown types.
/// When an unknown type is encountered, it will be resolved using the provided map.
/// If the unknown type has been resolved, it will be displayed with a ">=" prefix
/// to indicate it represents "at least" this type.
///
/// # Arguments
///
/// * `ty` - The type to convert to a string
/// * `resolved_unknown` - Map of unknown type IDs to their resolved concrete types
///
/// # Returns
///
/// A string representation of the type
pub(crate) fn type_display(ty: &Type, resolved_unknown: &HashMap<usize, Type>) -> String {
    use TypeKind::*;

    match &*ty.value {
        // Primitive types.
        I64 => "I64".to_string(),
        String => "String".to_string(),
        Bool => "Bool".to_string(),
        F64 => "F64".to_string(),

        // Special types.
        Unit => "()".to_string(),
        Universe => "Universe".to_string(),
        Nothing => "Nothing".to_string(),
        None => "None".to_string(),

        // Unknown types
        UnknownAsc(id) => {
            format!(
                "≧{{{}}}",
                type_display(resolved_unknown.get(id).unwrap(), resolved_unknown)
            )
        }
        UnknownDesc(id) => {
            format!(
                "≦{{{}}}",
                type_display(resolved_unknown.get(id).unwrap(), resolved_unknown)
            )
        }

        // User types.
        Adt(name) => name.to_string(),
        Generic(name) => name.to_string(),

        // Composite types.
        Array(elem) => format!("[{}]", type_display(elem, resolved_unknown)),
        Closure(param, ret) => format!(
            "{} -> {}",
            type_display(param, resolved_unknown),
            type_display(ret, resolved_unknown)
        ),
        Tuple(elems) => {
            if elems.is_empty() {
                "()".to_string()
            } else {
                let elem_strs: Vec<_> = elems
                    .iter()
                    .map(|elem| type_display(elem, resolved_unknown))
                    .collect();
                format!("({})", elem_strs.join(", "))
            }
        }
        Map(key, val) => format!(
            "{{{}:{}}}",
            type_display(key, resolved_unknown),
            type_display(val, resolved_unknown)
        ),
        Optional(inner) => format!("{}?", type_display(inner, resolved_unknown)),

        // Memo status types.
        Stored(inner) => format!("{}*", type_display(inner, resolved_unknown)),
        Costed(inner) => format!("{}$", type_display(inner, resolved_unknown)),

        // Native trait types.
        Concat => "Concat".to_string(),
        EqHash => "EqHash".to_string(),
        Arithmetic => "Arithmetic".to_string(),
    }
}

#[cfg(test)]
pub mod type_registry_tests {
    use super::*;
    use crate::dsl::{
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
        let return_type = TypeKind::I64.into();
        let result = create_function_type(&params, &return_type);
        match &*result.value {
            TypeKind::Closure(param, ret) => {
                assert_eq!(*param.value, TypeKind::Unit);
                assert_eq!(*ret.value, TypeKind::I64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with one parameter
        let params = vec![TypeKind::I64.into()];
        let result = create_function_type(&params, &return_type);
        match &*result.value {
            TypeKind::Closure(param, ret) => {
                assert_eq!(*param.value, TypeKind::I64);
                assert_eq!(*ret.value, TypeKind::I64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with multiple parameters
        let params = vec![TypeKind::I64.into(), TypeKind::Bool.into()];
        let result = create_function_type(&params, &return_type);
        match &*result.value {
            TypeKind::Closure(param, ret) => {
                match &*param.value {
                    TypeKind::Tuple(types) => {
                        assert_eq!(types.len(), 2);
                        assert_eq!(*types[0].value, TypeKind::I64);
                        assert_eq!(*types[1].value, TypeKind::Bool);
                    }
                    _ => panic!("Expected Tuple type for parameters"),
                }
                assert_eq!(*ret.value, TypeKind::I64);
            }
            _ => panic!("Expected Closure type"),
        }
    }
}
