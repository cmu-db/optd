use super::cycle_detect::CycleDetector;
use crate::dsl::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::Identifier,
        type_checks::registry::{CORE_TYPES, LOGICAL_TYPE, PHYSICAL_TYPE, TypeRegistry},
    },
    parser::ast::Type as AstType,
    utils::span::Span,
};
use std::collections::HashMap;

/// Checks that ADTs (Algebraic Data Types) are well-formed.
pub fn adt_check(registry: &TypeRegistry, src_path: &str) -> Result<(), Box<AnalyzerErrorKind>> {
    // First, check if types correctly reference each other and do not
    // create any infinite recursive cycles.
    let mut detector = CycleDetector::new(registry);

    for adt_name in registry.subtypes.keys() {
        if !detector.can_terminate(adt_name)? {
            // There might be multiple cycles in the path. However, we know that
            // the last element is part of a cycle, as it always corresponds to a field
            // of a product type that was already being explored. Therefore, we need to find
            // the previous occurrence of that element in the path, which indicates
            // the point at which it started being explored.
            if let Some(cycle_start_idx) = detector.find_cycle_start_index() {
                return Err(AnalyzerErrorKind::new_cyclic_type(
                    &detector.path[cycle_start_idx..],
                ));
            }
        }

        detector.reset();
    }

    // Second, check if the core types are defined, and are root types.
    validate_core_types(registry, src_path)?;

    // Finally, check if all fields are valid:
    // - No costed or stored types.
    // - Only logical types may contain logical children.
    // - Only physical types may contain physical children.
    // In fields:
    // - Logical types may only appear as [Logical] or Logical.
    // - Physical types may only appear as [Physical] or Physical.
    check_product_fields(registry)
}

/// Validates that core types are defined and are root types (not inherited by any other type).
fn validate_core_types(
    registry: &TypeRegistry,
    src_path: &str,
) -> Result<(), Box<AnalyzerErrorKind>> {
    // First check if all core types are defined.
    for core_type in CORE_TYPES {
        if !registry.subtypes.contains_key(core_type) {
            return Err(AnalyzerErrorKind::new_missing_core_type(
                core_type, src_path,
            ));
        }
    }

    // Then check that no core type is a child of another type.
    for (parent, children) in registry.subtypes.iter() {
        for core_type in CORE_TYPES {
            if children.contains(core_type) {
                return Err(AnalyzerErrorKind::new_invalid_inheritance(
                    parent,
                    registry.spans.get(parent).unwrap(),
                    core_type,
                    registry.spans.get(core_type).unwrap(),
                ));
            }
        }
    }

    Ok(())
}

fn check_product_fields(registry: &TypeRegistry) -> Result<(), Box<AnalyzerErrorKind>> {
    for (adt_name, fields) in &registry.product_fields {
        let mut field_names: HashMap<_, Span> = HashMap::new();

        for field in fields {
            let field_name = field.name.value.as_str();

            if let Some(first_span) = field_names.get(field_name) {
                return Err(AnalyzerErrorKind::new_duplicate_identifier(
                    field_name,
                    first_span,
                    &field.name.span,
                ));
            } else {
                field_names.insert(field_name.to_string(), field.name.span.clone());
            }

            check_type(
                &field.ty.value,
                &field.ty.span,
                registry,
                registry.inherits_adt(adt_name, LOGICAL_TYPE),
                registry.inherits_adt(adt_name, PHYSICAL_TYPE),
            )?;
        }
    }

    Ok(())
}

fn valid_core_type(
    ty: &Identifier,
    span: &Span,
    registry: &TypeRegistry,
    allows_logical: bool,
    allows_physical: bool,
) -> Result<(), Box<AnalyzerErrorKind>> {
    // Check if the type is logical but not allowed to be logical,
    // or is physical but not allowed to be physical.
    let invalid_logical = registry.inherits_adt(ty, LOGICAL_TYPE) && !allows_logical;
    let invalid_physical = registry.inherits_adt(ty, PHYSICAL_TYPE) && !allows_physical;

    if invalid_logical || invalid_physical {
        return Err(AnalyzerErrorKind::new_invalid_type(span));
    }

    Ok(())
}

fn check_type(
    ty: &AstType,
    span: &Span,
    registry: &TypeRegistry,
    allows_logical: bool,
    allows_physical: bool,
) -> Result<(), Box<AnalyzerErrorKind>> {
    use AstType::*;

    match ty {
        Identifier(name) => valid_core_type(name, span, registry, allows_logical, allows_physical),

        Array(elem_type) => match &*elem_type.value {
            Identifier(name) => {
                valid_core_type(name, span, registry, allows_logical, allows_physical)
            }
            _ => check_type(&elem_type.value, &elem_type.span, registry, false, false),
        },

        Tuple(types) => types
            .iter()
            .try_for_each(|elem| check_type(&elem.value, &elem.span, registry, false, false)),

        Map(key_type, val_type) => {
            check_type(&key_type.value, &key_type.span, registry, false, false)?;
            check_type(&val_type.value, &val_type.span, registry, false, false)
        }

        Closure(param_type, ret_type) => {
            check_type(&param_type.value, &param_type.span, registry, false, false)?;
            check_type(&ret_type.value, &ret_type.span, registry, false, false)
        }

        Questioned(inner_type) => {
            check_type(&inner_type.value, &inner_type.span, registry, false, false)
        }

        Dollared(_) => Err(AnalyzerErrorKind::new_invalid_type(span)),

        Starred(_) => Err(AnalyzerErrorKind::new_invalid_type(span)),

        Unknown => Err(AnalyzerErrorKind::new_invalid_type(span)),

        _ => Ok(()), // Primitive types don't contain logical or physical.
    }
}

#[cfg(test)]
mod adt_validation_tests {
    use crate::dsl::analyzer::hir::Identifier;
    use crate::dsl::analyzer::semantic_checks::adt_check::adt_check;
    use crate::dsl::analyzer::type_checks::registry::{
        LOGICAL_PROPS, LOGICAL_TYPE, PHYSICAL_PROPS, PHYSICAL_TYPE, TypeRegistry,
    };
    use crate::dsl::parser::ast::{Field, Type as AstType};
    use crate::dsl::utils::span::{Span, Spanned};
    use std::collections::{BTreeMap, HashMap, HashSet};

    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    // Helper to create a TypeRegistry with predefined types for testing
    fn setup_test_registry(
        subtypes: BTreeMap<Identifier, HashSet<String>>,
        product_fields: HashMap<Identifier, Vec<Field>>,
    ) -> TypeRegistry {
        // Populate spans for all mentioned types
        let mut spans = HashMap::new();
        for name in subtypes.keys() {
            spans.insert(name.clone(), create_test_span());
        }
        for name in product_fields.keys() {
            spans.insert(name.clone(), create_test_span());
        }

        TypeRegistry {
            spans,
            subtypes,
            product_fields,
            ty_return: None,
            constraints: vec![],
            resolved_unknown: HashMap::new(),
            next_id: 0,
        }
    }

    // Helper to create an AdtField
    fn create_field(name: &str, ty: AstType) -> Field {
        Field {
            name: Spanned::new(name.to_string(), create_test_span()),
            ty: Spanned::new(ty, create_test_span()),
        }
    }

    // Helper to create a registry with all core types defined
    fn create_core_registry() -> (
        BTreeMap<Identifier, HashSet<String>>,
        HashMap<Identifier, Vec<Field>>,
    ) {
        let mut subtypes = BTreeMap::new();

        // Define the core types
        subtypes.insert(LOGICAL_TYPE.to_string(), HashSet::new());
        subtypes.insert(PHYSICAL_TYPE.to_string(), HashSet::new());
        subtypes.insert(LOGICAL_PROPS.to_string(), HashSet::new());
        subtypes.insert(PHYSICAL_PROPS.to_string(), HashSet::new());

        let product_fields = HashMap::new();

        (subtypes, product_fields)
    }

    #[test]
    fn test_direct_recursion_in_product() {
        // Test a direct recursive product type: type Node(value: Int64, next: Node)
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("Node".to_string(), HashSet::new());

        product_fields.insert(
            "Node".to_string(),
            vec![
                create_field("value", AstType::Int64),
                create_field("next", AstType::Identifier("Node".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    #[test]
    fn test_valid_recursive_sum_type() {
        // Test a valid recursive sum type with a base case:
        // type List =
        //   | Nil
        //   \ Cons(value: Int64, next: List)
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Set up subtypes
        let mut list_variants = HashSet::new();
        list_variants.insert("Nil".to_string());
        list_variants.insert("Cons".to_string());
        subtypes.insert("List".to_string(), list_variants);

        // Set up fields
        product_fields.insert("Nil".to_string(), vec![]);
        product_fields.insert(
            "Cons".to_string(),
            vec![
                create_field("value", AstType::Int64),
                create_field("next", AstType::Identifier("List".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_recursive_sum_type() {
        // Test an invalid recursive sum type without a base case:
        // type BadList =
        //   | ConsA(next: BadList)
        //   \ ConsB(next: BadList)
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Set up subtypes
        let mut bad_list_variants = HashSet::new();
        bad_list_variants.insert("ConsA".to_string());
        bad_list_variants.insert("ConsB".to_string());
        subtypes.insert("BadList".to_string(), bad_list_variants);

        // Set up fields
        product_fields.insert(
            "ConsA".to_string(),
            vec![create_field(
                "next",
                AstType::Identifier("BadList".to_string()),
            )],
        );
        product_fields.insert(
            "ConsB".to_string(),
            vec![create_field(
                "next",
                AstType::Identifier("BadList".to_string()),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    #[test]
    fn test_indirect_recursion() {
        // Test indirect recursion:
        // type A(b: B)
        // type B(a: A)
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("A".to_string(), HashSet::new());
        subtypes.insert("B".to_string(), HashSet::new());

        product_fields.insert(
            "A".to_string(),
            vec![create_field("b", AstType::Identifier("B".to_string()))],
        );
        product_fields.insert(
            "B".to_string(),
            vec![create_field("a", AstType::Identifier("A".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    #[test]
    fn test_nested_type_recursion() {
        // Test recursion in nested types:
        // type Nested(arr: Array[Nested])
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("Nested".to_string(), HashSet::new());

        product_fields.insert(
            "Nested".to_string(),
            vec![create_field(
                "arr",
                AstType::Array(Spanned::new(
                    AstType::Identifier("Nested".to_string()),
                    create_test_span(),
                )),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    #[test]
    fn test_non_recursive_types() {
        // Test non-recursive types:
        // type Point(x: Int64, y: Int64)
        // type Rectangle(topLeft: Point, bottomRight: Point)
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("Point".to_string(), HashSet::new());
        subtypes.insert("Rectangle".to_string(), HashSet::new());

        product_fields.insert(
            "Point".to_string(),
            vec![
                create_field("x", AstType::Int64),
                create_field("y", AstType::Int64),
            ],
        );
        product_fields.insert(
            "Rectangle".to_string(),
            vec![
                create_field("topLeft", AstType::Identifier("Point".to_string())),
                create_field("bottomRight", AstType::Identifier("Point".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_ok());
    }

    #[test]
    fn test_nested_list_with_cycles() {
        // This test represents the structure:
        //
        // type A =
        //    \ List =
        //      | Bla(b: List)
        //      \ Vla(a: List)
        let (mut subtypes, mut product_fields) = create_core_registry();

        // A has List as its only variant
        let mut a_variants = HashSet::new();
        a_variants.insert("List".to_string());
        subtypes.insert("A".to_string(), a_variants);

        // List has two variants: Bla and Vla
        let mut list_variants = HashSet::new();
        list_variants.insert("Bla".to_string());
        list_variants.insert("Vla".to_string());
        subtypes.insert("List".to_string(), list_variants);

        // Set up fields
        product_fields.insert(
            "Bla".to_string(),
            vec![create_field("b", AstType::Identifier("List".to_string()))],
        );
        product_fields.insert(
            "Vla".to_string(),
            vec![create_field("a", AstType::Identifier("List".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err()); // Should detect the cycle
    }

    #[test]
    fn test_optional_recursive_type() {
        // Test a recursive type that uses Optional to break the recursion:
        // type LinkedList(value: Int64, next: LinkedList?)
        // This should be valid because Optional (?) breaks the recursion.
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("LinkedList".to_string(), HashSet::new());

        product_fields.insert(
            "LinkedList".to_string(),
            vec![
                create_field("value", AstType::Int64),
                create_field(
                    "next",
                    AstType::Questioned(Spanned::new(
                        AstType::Identifier("LinkedList".to_string()),
                        create_test_span(),
                    )),
                ),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_ok());
    }

    #[test]
    fn test_complex_expression_language() {
        // Create a simplified expression language:
        //
        // type Expr =
        //   | Literal(value: Int64)
        //   | BinOp(left: Expr, right: Expr, op: String)
        //   \ Condition(cond: Predicate)
        //
        // type Predicate =
        //   | Equals(left: Expr, right: Expr)
        //   \ And(left: Predicate, right: Predicate)
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Expr variants
        let mut expr_variants = HashSet::new();
        expr_variants.insert("Literal".to_string());
        expr_variants.insert("BinOp".to_string());
        expr_variants.insert("Condition".to_string());
        subtypes.insert("Expr".to_string(), expr_variants);

        // Predicate variants
        let mut predicate_variants = HashSet::new();
        predicate_variants.insert("Equals".to_string());
        predicate_variants.insert("And".to_string());
        subtypes.insert("Predicate".to_string(), predicate_variants);

        // Set up fields
        // Terminating variant for Expr
        product_fields.insert(
            "Literal".to_string(),
            vec![create_field("value", AstType::Int64)],
        );

        // Recursive variants
        product_fields.insert(
            "BinOp".to_string(),
            vec![
                create_field("left", AstType::Identifier("Expr".to_string())),
                create_field("right", AstType::Identifier("Expr".to_string())),
                create_field("op", AstType::String),
            ],
        );

        product_fields.insert(
            "Condition".to_string(),
            vec![create_field(
                "cond",
                AstType::Identifier("Predicate".to_string()),
            )],
        );

        product_fields.insert(
            "Equals".to_string(),
            vec![
                create_field("left", AstType::Identifier("Expr".to_string())),
                create_field("right", AstType::Identifier("Expr".to_string())),
            ],
        );

        product_fields.insert(
            "And".to_string(),
            vec![
                create_field("left", AstType::Identifier("Predicate".to_string())),
                create_field("right", AstType::Identifier("Predicate".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_ok()); // Valid because Literal is a terminating variant for Expr
    }

    #[test]
    fn test_nested_sum_only_recursive() {
        // Test a structure with nested sum types where all paths lead to recursion:
        //
        // type A =
        //   \ B =
        //     \ C(a: A)
        let (mut subtypes, mut product_fields) = create_core_registry();

        // A has B as its only variant
        let mut a_variants = HashSet::new();
        a_variants.insert("B".to_string());
        subtypes.insert("A".to_string(), a_variants);

        // B has C as its only variant
        let mut b_variants = HashSet::new();
        b_variants.insert("C".to_string());
        subtypes.insert("B".to_string(), b_variants);

        // C references back to A, creating a cycle
        product_fields.insert(
            "C".to_string(),
            vec![create_field("a", AstType::Identifier("A".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err()); // Should detect the cycle and fail
    }

    #[test]
    fn test_duplicate_fields() {
        // Test a product type with duplicate field names:
        // type DuplicateField(x: Int64, x: String)
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("DuplicateField".to_string(), HashSet::new());

        let fields = vec![
            create_field("x", AstType::Int64),
            create_field("x", AstType::String), // Duplicate field name
        ];

        product_fields.insert("DuplicateField".to_string(), fields);

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    #[test]
    fn test_undefined_type_reference() {
        // Test a product type that references an undefined type:
        // type BadReference(field: UndefinedType)
        let (mut subtypes, mut product_fields) = create_core_registry();

        subtypes.insert("BadReference".to_string(), HashSet::new());

        product_fields.insert(
            "BadReference".to_string(),
            vec![create_field(
                "field",
                AstType::Identifier("UndefinedType".to_string()),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    #[test]
    fn test_complex_type_with_duplicate_nested_fields() {
        // Test a more complex type with duplicate fields in nested structures
        // type Complex =
        //   | VariantA(x: Int64, y: String)
        //   \ VariantB(x: Int64, x: Bool)  // Duplicate field in this variant
        let (mut subtypes, mut product_fields) = create_core_registry();

        let mut complex_variants = HashSet::new();
        complex_variants.insert("VariantA".to_string());
        complex_variants.insert("VariantB".to_string());
        subtypes.insert("Complex".to_string(), complex_variants);

        product_fields.insert(
            "VariantA".to_string(),
            vec![
                create_field("x", AstType::Int64),
                create_field("y", AstType::String),
            ],
        );

        product_fields.insert(
            "VariantB".to_string(),
            vec![
                create_field("x", AstType::Int64),
                create_field("x", AstType::Bool), // Duplicate field
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        assert!(result.is_err());
    }

    // New tests for core types and type validation

    #[test]
    fn test_missing_core_types() {
        // Create a registry with no types
        let subtypes = BTreeMap::new();
        let product_fields = HashMap::new();

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        // Should fail because core types are missing
        assert!(result.is_err());

        // Create a registry with only some core types
        let mut subtypes = BTreeMap::new();
        subtypes.insert(LOGICAL_TYPE.to_string(), HashSet::new());
        subtypes.insert(PHYSICAL_TYPE.to_string(), HashSet::new());
        // Missing LOGICAL_PROPS and PHYSICAL_PROPS

        let registry = setup_test_registry(subtypes, HashMap::new());
        let result = adt_check(&registry, "test.opt");

        // Should fail because some core types are missing
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_inheritance_of_core_types() {
        // Create a registry where a type tries to inherit from a core type
        let mut subtypes = BTreeMap::new();

        // Define the core types
        subtypes.insert(LOGICAL_TYPE.to_string(), HashSet::new());
        subtypes.insert(PHYSICAL_TYPE.to_string(), HashSet::new());
        subtypes.insert(LOGICAL_PROPS.to_string(), HashSet::new());
        subtypes.insert(PHYSICAL_PROPS.to_string(), HashSet::new());

        // Create a type that tries to have LOGICAL_TYPE as a child (invalid)
        let mut invalid_parent_variants = HashSet::new();
        invalid_parent_variants.insert(LOGICAL_TYPE.to_string());
        subtypes.insert("InvalidParent".to_string(), invalid_parent_variants);

        let registry = setup_test_registry(subtypes, HashMap::new());
        let result = adt_check(&registry, "test.opt");

        // Should fail because a type is trying to inherit from a core type
        assert!(result.is_err());
    }

    #[test]
    fn test_logical_and_physical_field_types() {
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Create types inheriting from Logical
        let mut logical_subtypes = HashSet::new();
        logical_subtypes.insert("LogicalOperator".to_string());
        logical_subtypes.insert("LogicalChild".to_string());
        subtypes.insert(LOGICAL_TYPE.to_string(), logical_subtypes);

        // Create types inheriting from Physical
        let mut physical_subtypes = HashSet::new();
        physical_subtypes.insert("PhysicalOperator".to_string());
        physical_subtypes.insert("PhysicalChild".to_string());
        subtypes.insert(PHYSICAL_TYPE.to_string(), physical_subtypes);

        // Add empty subtypes entries for leaf types
        subtypes.insert("LogicalOperator".to_string(), HashSet::new());
        subtypes.insert("LogicalChild".to_string(), HashSet::new());
        subtypes.insert("PhysicalOperator".to_string(), HashSet::new());
        subtypes.insert("PhysicalChild".to_string(), HashSet::new());

        // Valid: Logical operator with a different logical type field
        product_fields.insert(
            "LogicalOperator".to_string(),
            vec![create_field(
                "child",
                AstType::Identifier("LogicalChild".to_string()),
            )],
        );

        // Add fields for LogicalChild
        product_fields.insert(
            "LogicalChild".to_string(),
            vec![create_field("value", AstType::Int64)],
        );

        // Valid: Physical operator with a different physical type field
        product_fields.insert(
            "PhysicalOperator".to_string(),
            vec![create_field(
                "child",
                AstType::Identifier("PhysicalChild".to_string()),
            )],
        );

        // Add fields for PhysicalChild
        product_fields.insert(
            "PhysicalChild".to_string(),
            vec![create_field("value", AstType::Int64)],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        // Should pass because types are used correctly
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_logical_in_non_logical_parent() {
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Add a regular type (not inheriting from Logical or Physical)
        subtypes.insert("RegularType".to_string(), HashSet::new());

        // Invalid: Regular type with logical field
        product_fields.insert(
            "RegularType".to_string(),
            vec![create_field(
                "child",
                AstType::Identifier(LOGICAL_TYPE.to_string()),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        // Should fail because a regular type cannot contain a logical field
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_physical_in_non_physical_parent() {
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Add a regular type (not inheriting from Logical or Physical)
        subtypes.insert("RegularType".to_string(), HashSet::new());

        // Invalid: Regular type with physical field
        product_fields.insert(
            "RegularType".to_string(),
            vec![create_field(
                "child",
                AstType::Identifier(PHYSICAL_TYPE.to_string()),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        // Should fail because a regular type cannot contain a physical field
        assert!(result.is_err());
    }

    #[test]
    fn test_array_of_logical_and_physical() {
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Create types inheriting from Logical and Physical
        let mut logical_subtypes = HashSet::new();
        logical_subtypes.insert("LogicalOperator".to_string());
        logical_subtypes.insert("OtherLogical".to_string());
        subtypes.insert(LOGICAL_TYPE.to_string(), logical_subtypes);

        let mut physical_subtypes = HashSet::new();
        physical_subtypes.insert("PhysicalOperator".to_string());
        physical_subtypes.insert("OtherPhysical".to_string());
        subtypes.insert(PHYSICAL_TYPE.to_string(), physical_subtypes);

        // Add empty subtypes entries for leaf types
        subtypes.insert("LogicalOperator".to_string(), HashSet::new());
        subtypes.insert("OtherLogical".to_string(), HashSet::new());
        subtypes.insert("PhysicalOperator".to_string(), HashSet::new());
        subtypes.insert("OtherPhysical".to_string(), HashSet::new());

        // Valid: Array of Logical in Logical type
        product_fields.insert(
            "LogicalOperator".to_string(),
            vec![create_field(
                "children",
                AstType::Array(Spanned::new(
                    AstType::Identifier("OtherLogical".to_string()),
                    create_test_span(),
                )),
            )],
        );

        // Give OtherLogical a primitive field
        product_fields.insert(
            "OtherLogical".to_string(),
            vec![create_field("value", AstType::Int64)],
        );

        // Valid: Array of Physical in Physical type
        product_fields.insert(
            "PhysicalOperator".to_string(),
            vec![create_field(
                "children",
                AstType::Array(Spanned::new(
                    AstType::Identifier("OtherPhysical".to_string()),
                    create_test_span(),
                )),
            )],
        );

        // Give OtherPhysical a primitive field
        product_fields.insert(
            "OtherPhysical".to_string(),
            vec![create_field("value", AstType::Int64)],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        // Should pass because arrays of Logical/Physical are valid
        assert!(result.is_ok());
    }

    #[test]
    fn test_nested_array_of_logical_invalid() {
        let (mut subtypes, mut product_fields) = create_core_registry();

        // Create a type inheriting from Logical
        let mut logical_subtypes = HashSet::new();
        logical_subtypes.insert("LogicalOperator".to_string());
        subtypes.insert(LOGICAL_TYPE.to_string(), logical_subtypes);

        // Invalid: Nested array of Logical
        product_fields.insert(
            "LogicalOperator".to_string(),
            vec![create_field(
                "badField",
                AstType::Array(Spanned::new(
                    AstType::Array(Spanned::new(
                        AstType::Identifier(LOGICAL_TYPE.to_string()),
                        create_test_span(),
                    )),
                    create_test_span(),
                )),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry, "test.opt");

        // Should fail because nested arrays of Logical are invalid
        assert!(result.is_err());
    }
}
