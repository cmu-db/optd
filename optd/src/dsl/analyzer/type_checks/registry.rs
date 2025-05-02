use crate::dsl::analyzer::errors::AnalyzerErrorKind;
use crate::dsl::analyzer::hir::{Identifier, Pattern, TypedSpan};
use crate::dsl::parser::ast::{Adt, Field};
use crate::dsl::utils::span::{OptionalSpanned, Span};
use Adt::*;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use super::converter::convert_ast_type;

// Core type constants.
// These are expressed as strings, not a Type enum, because they are
// regular ADTs in the language.
pub const LOGICAL_TYPE: &str = "Logical";
pub const PHYSICAL_TYPE: &str = "Physical";
pub const LOGICAL_PROPS: &str = "LogicalProperties";
pub const PHYSICAL_PROPS: &str = "PhysicalProperties";

pub const CORE_TYPES: [&str; 4] = [LOGICAL_TYPE, PHYSICAL_TYPE, LOGICAL_PROPS, PHYSICAL_PROPS];

// Reserved ADT type instances.
pub const ARITHMERIC_TYPE: &str = "Arithmetic";
pub const CONCAT_TYPE: &str = "Concat";
pub const EQHASH_TYPE: &str = "EqHash";

pub static RESERVED_TYPE_MAP: Lazy<HashMap<String, TypeKind>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert(CONCAT_TYPE.to_string(), TypeKind::Concat);
    map.insert(EQHASH_TYPE.to_string(), TypeKind::EqHash);
    map.insert(ARITHMERIC_TYPE.to_string(), TypeKind::Arithmetic);
    map
});

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
    Gen(Generic),

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

/// Represents a generic type with an optional bound.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Generic(pub usize, pub Option<Type>);

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

impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        *self.value == *other.value
    }
}

impl Eq for Type {}

impl Hash for Type {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
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
        id: usize, // To track generics in `solver.rs`.
        inner: TypedSpan,
        args: Vec<TypedSpan>,
        outer: TypedSpan,
    },

    /// Pattern match: `scrutinee >: pattern`
    ///
    /// This constraint enforces that the type of `scrutinee` is compatible with
    /// the pattern `pattern`. It does not only check for subtyping, but also
    /// ensures that the pattern can be destructured from the scrutinee.
    ///
    /// (e.g. in the context of list decomposition, * and $ handling, etc.)
    Scrutinee {
        scrutinee: TypedSpan,
        pattern: Pattern<TypedSpan>,
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
    /// Instantiated generic types: call_constraint_id -> Vec<(Generic, TypeKind)>.
    pub instantiated_generics: HashMap<usize, Vec<(Generic, TypeKind)>>,
    /// Current ID to use for new Unknown / Generic / Constraint id.
    pub next_id: usize,
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
            instantiated_generics: HashMap::new(),
            next_id: 0,
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
        let check_name_validity = |name, span, registry: &TypeRegistry| {
            if RESERVED_TYPE_MAP.contains_key(name) {
                return Err(AnalyzerErrorKind::new_reserved_type(name, span));
            }

            if let Some(existing_span) = registry.spans.get(name) {
                return Err(AnalyzerErrorKind::new_duplicate_adt(
                    name,
                    existing_span,
                    span,
                ));
            }

            Ok(())
        };

        match adt {
            Product { name, fields } => {
                let type_name = name.value.as_ref().clone();
                check_name_validity(&type_name, &name.span, self)?;

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
                check_name_validity(&enum_name, &name.span, self)?;

                self.spans.insert(enum_name.clone(), name.clone().span);
                self.subtypes.entry(enum_name.clone()).or_default();

                for variant in variants {
                    let variant_adt = variant.value.as_ref();
                    self.register_adt(variant_adt)?;

                    let variant_name = match variant_adt {
                        Product { name, .. } => name.value.as_ref(),
                        Sum { name, .. } => name.value.as_ref(),
                    };

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
    pub fn get_product_field_type_by_index(&self, adt_name: &str, index: usize) -> Option<Type> {
        let fields = self.product_fields.get(adt_name)?;
        let field = fields.get(index).cloned()?;

        Some(convert_ast_type(field.ty))
    }

    /// Creates a new unknown, ascending type.
    pub fn new_unknown_asc(&mut self) -> TypeKind {
        use TypeKind::*;

        let id = self.next_id;
        self.next_id += 1;
        self.resolved_unknown.insert(id, Nothing.into());
        UnknownAsc(id)
    }

    /// Creates a new unknown, descending type.
    pub fn new_unknown_desc(&mut self) -> TypeKind {
        use TypeKind::*;

        let id = self.next_id;
        self.next_id += 1;
        self.resolved_unknown.insert(id, Universe.into());
        UnknownDesc(id)
    }

    /// Adds a subtyping constraint set: `parent >: all children`
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
            id: self.next_id,
            inner: inner.clone(),
            args: args.to_vec(),
            outer: outer.clone(),
        });
        self.next_id += 1;
    }

    /// Adds a pattern match constraint: `scrutinee >: pattern`
    ///
    /// # Arguments
    ///
    /// * `scrutinee` - The type of the scrutinee being matched
    /// * `pattern` - The pattern being matched against the scrutinee
    pub(super) fn add_constraint_scrutinee(
        &mut self,
        scrutinee: &TypedSpan,
        pattern: &Pattern<TypedSpan>,
    ) {
        self.constraints.push(Constraint::Scrutinee {
            scrutinee: scrutinee.clone(),
            pattern: pattern.clone(),
        });
    }

    /// Adds a field access constraint: `outer >: inner.field`
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

    /// Instantiates a type by replacing all generic type references with concrete types
    /// specific to the given constraint.
    ///
    /// When a generic type is encountered, the function looks for an existing instantiation
    /// for the (constraint_id, generic_id) pair. If none exists, it creates a new ascending
    /// or descending unknown type and stores it in the instantiated_generics map.
    ///
    /// # Arguments
    ///
    /// * `ty` - The type to instantiate
    /// * `constraint_id` - The ID of the constraint that needs this instantiation
    /// * `ascending` - Whether unknown types should be created as ascending (true) or descending (false)
    ///
    /// # Returns
    ///
    /// A new type with all generic references replaced with constraint-specific instantiations.
    pub fn instantiate_type(&mut self, ty: &Type, constraint_id: usize, ascending: bool) -> Type {
        use TypeKind::*;

        let span = ty.span.clone();

        let kind = match &*ty.value {
            Gen(generic) => self
                .instantiated_generics
                .get(&constraint_id)
                .and_then(|vec| {
                    vec.iter()
                        .find(|(g, _)| g == generic)
                        .map(|(_, ty)| ty.clone())
                })
                .unwrap_or_else(|| {
                    let next_id = if ascending {
                        self.new_unknown_asc()
                    } else {
                        self.new_unknown_desc()
                    };

                    self.instantiated_generics
                        .entry(constraint_id)
                        .or_default()
                        .push((generic.clone(), next_id.clone()));

                    next_id
                }),

            Array(elem) => Array(self.instantiate_type(elem, constraint_id, ascending)),

            Closure(param, ret) => Closure(
                self.instantiate_type(param, constraint_id, !ascending),
                self.instantiate_type(ret, constraint_id, ascending),
            ),

            Tuple(types) => Tuple(
                types
                    .iter()
                    .map(|t| self.instantiate_type(t, constraint_id, ascending))
                    .collect(),
            ),

            Map(k, v) => Map(
                self.instantiate_type(k, constraint_id, !ascending),
                self.instantiate_type(v, constraint_id, ascending),
            ),

            Optional(inner) => Optional(self.instantiate_type(inner, constraint_id, ascending)),
            Stored(inner) => Stored(self.instantiate_type(inner, constraint_id, ascending)),
            Costed(inner) => Costed(self.instantiate_type(inner, constraint_id, ascending)),

            other => other.clone(),
        };

        Type {
            value: kind.into(),
            span,
        }
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
    pub(crate) fn resolve_type(&self, ty: &Type) -> Type {
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

#[cfg(test)]
pub mod type_registry_tests {
    use super::*;
    use crate::dsl::{
        parser::ast::{Field, Type as AstType},
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
    fn test_reserved_type_detection() {
        let mut registry = TypeRegistry::default();

        // Try to register an ADT with a reserved name
        let concat_type = create_product_adt(CONCAT_TYPE, vec![]);
        let result = registry.register_adt(&concat_type);
        assert!(result.is_err());

        if let Err(err) = result {
            match *err {
                AnalyzerErrorKind::ReservedType { name, .. } => {
                    assert_eq!(name, CONCAT_TYPE);
                }
                _ => panic!("Expected ReservedType error"),
            }
        }

        // Also try with Arithmetic
        let arithmetic_type = create_product_adt(ARITHMERIC_TYPE, vec![]);
        let result = registry.register_adt(&arithmetic_type);
        assert!(result.is_err());

        if let Err(err) = result {
            match *err {
                AnalyzerErrorKind::ReservedType { name, .. } => {
                    assert_eq!(name, ARITHMERIC_TYPE);
                }
                _ => panic!("Expected ReservedType error"),
            }
        }

        // Normal type registration should still work
        let valid_type = create_product_adt("ValidType", vec![]);
        assert!(registry.register_adt(&valid_type).is_ok());
    }
}
