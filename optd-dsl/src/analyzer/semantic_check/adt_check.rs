use super::cycle_detect::CycleDetector;
use crate::{
    analyzer::{error::AnalyzerErrorKind, types::TypeRegistry},
    utils::span::Span,
};
use std::collections::HashMap;

/// Checks that ADTs (Algebraic Data Types) are well-formed
pub fn adt_check(registry: &TypeRegistry) -> Result<(), AnalyzerErrorKind> {
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
                return Err(AnalyzerErrorKind::new_cyclic_adt(
                    &detector.path[cycle_start_idx..],
                ));
            }
        }

        detector.reset();
    }

    // Second, check for duplicate fields in product types.
    check_duplicate_fields(registry)?;

    Ok(())
}

/// Checks for duplicate fields in product types
fn check_duplicate_fields(registry: &TypeRegistry) -> Result<(), AnalyzerErrorKind> {
    for fields in registry.product_fields.values() {
        let mut field_names: HashMap<_, Span> = HashMap::new();

        for field in fields {
            let field_name = field.name.value.as_str();

            if let Some(first_span) = field_names.get(field_name) {
                return Err(AnalyzerErrorKind::new_duplicate_identifier(
                    field_name.to_string(),
                    first_span.clone(),
                    field.name.span.clone(),
                ));
            } else {
                field_names.insert(field_name.to_string(), field.name.span.clone());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod adt_cycle_tests {
    use crate::analyzer::hir::Identifier;
    use crate::analyzer::semantic_check::adt_check::adt_check;
    use crate::analyzer::types::TypeRegistry;
    use crate::parser::ast::{Field, Type};
    use crate::utils::span::{Span, Spanned};
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
        }
    }

    // Helper to create an AdtField
    fn create_field(name: &str, ty: Type) -> Field {
        Field {
            name: Spanned::new(name.to_string(), create_test_span()),
            ty: Spanned::new(ty, create_test_span()),
        }
    }

    #[test]
    fn test_direct_recursion_in_product() {
        // Test a direct recursive product type: type Node(value: Int64, next: Node)
        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Node".to_string(),
            vec![
                create_field("value", Type::Int64),
                create_field("next", Type::Identifier("Node".to_string())),
            ],
        );

        let mut subtypes = BTreeMap::new();
        subtypes.insert("Node".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_valid_recursive_sum_type() {
        // Test a valid recursive sum type with a base case:
        // type List =
        //   | Nil
        //   | Cons(value: Int64, next: List)

        // Set up subtypes
        let mut subtypes = BTreeMap::new();
        let mut list_variants = HashSet::new();
        list_variants.insert("Nil".to_string());
        list_variants.insert("Cons".to_string());
        subtypes.insert("List".to_string(), list_variants);

        // Set up fields
        let mut product_fields = HashMap::new();
        product_fields.insert("Nil".to_string(), vec![]);
        product_fields.insert(
            "Cons".to_string(),
            vec![
                create_field("value", Type::Int64),
                create_field("next", Type::Identifier("List".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_recursive_sum_type() {
        // Test an invalid recursive sum type without a base case:
        // type BadList =
        //   | ConsA(next: BadList)
        //   | ConsB(next: BadList)

        // Set up subtypes
        let mut subtypes = BTreeMap::new();
        let mut bad_list_variants = HashSet::new();
        bad_list_variants.insert("ConsA".to_string());
        bad_list_variants.insert("ConsB".to_string());
        subtypes.insert("BadList".to_string(), bad_list_variants);

        // Set up fields
        let mut product_fields = HashMap::new();
        product_fields.insert(
            "ConsA".to_string(),
            vec![create_field(
                "next",
                Type::Identifier("BadList".to_string()),
            )],
        );
        product_fields.insert(
            "ConsB".to_string(),
            vec![create_field(
                "next",
                Type::Identifier("BadList".to_string()),
            )],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_indirect_recursion() {
        // Test indirect recursion:
        // type A(b: B)
        // type B(a: A)

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "A".to_string(),
            vec![create_field("b", Type::Identifier("B".to_string()))],
        );
        product_fields.insert(
            "B".to_string(),
            vec![create_field("a", Type::Identifier("A".to_string()))],
        );

        let mut subtypes = BTreeMap::new();
        subtypes.insert("A".to_string(), HashSet::new());
        subtypes.insert("B".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_nested_type_recursion() {
        // Test recursion in nested types:
        // type Nested(arr: Array[Nested])

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Nested".to_string(),
            vec![create_field(
                "arr",
                Type::Array(Spanned::new(
                    Type::Identifier("Nested".to_string()),
                    create_test_span(),
                )),
            )],
        );

        let mut subtypes = BTreeMap::new();
        subtypes.insert("Nested".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_non_recursive_types() {
        // Test non-recursive types:
        // type Point(x: Int64, y: Int64)
        // type Rectangle(topLeft: Point, bottomRight: Point)

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Point".to_string(),
            vec![
                create_field("x", Type::Int64),
                create_field("y", Type::Int64),
            ],
        );
        product_fields.insert(
            "Rectangle".to_string(),
            vec![
                create_field("topLeft", Type::Identifier("Point".to_string())),
                create_field("bottomRight", Type::Identifier("Point".to_string())),
            ],
        );

        let mut subtypes = BTreeMap::new();
        subtypes.insert("Point".to_string(), HashSet::new());
        subtypes.insert("Rectangle".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_level_nested_adt() {
        // This test replicates a complex multi-level ADT structure similar to:
        //
        // type Physical =
        //   | Scan(table_name: String)  // Terminating variant
        //   | PhysFilter(child: Physical, cond: Predicate)
        //   | PhysProject(child: Physical, exprs: [Scalar])
        //   \ PhysJoin =
        //       | HashJoin(build_side: Physical, probe_side: Physical, typ: String, cond: Predicate)
        //       | MergeJoin(left: Physical, right: Physical, typ: String, cond: Predicate)
        //       \ NestedLoopJoin(outer: Physical, inner: Physical, typ: String, cond: Predicate)
        //
        // type Predicate =
        //   \ Equals(left: Scalar, right: Scalar)  // Terminating variant
        //
        // type Scalar =
        //   \ Literal(value: Int64)  // Terminating variant

        // Set up the type hierarchy
        let mut subtypes = BTreeMap::new();

        // Physical has 4 variants
        let mut physical_variants = HashSet::new();
        physical_variants.insert("Scan".to_string());
        physical_variants.insert("PhysFilter".to_string());
        physical_variants.insert("PhysProject".to_string());
        physical_variants.insert("PhysJoin".to_string());
        subtypes.insert("Physical".to_string(), physical_variants);

        // PhysJoin has 3 variants
        let mut phys_join_variants = HashSet::new();
        phys_join_variants.insert("HashJoin".to_string());
        phys_join_variants.insert("MergeJoin".to_string());
        phys_join_variants.insert("NestedLoopJoin".to_string());
        subtypes.insert("PhysJoin".to_string(), phys_join_variants);

        // Set up basic types for Predicate and Scalar
        let mut predicate_variants = HashSet::new();
        predicate_variants.insert("Equals".to_string());
        subtypes.insert("Predicate".to_string(), predicate_variants);

        let mut scalar_variants = HashSet::new();
        scalar_variants.insert("Literal".to_string());
        subtypes.insert("Scalar".to_string(), scalar_variants);

        // Set up the field definitions
        let mut product_fields = HashMap::new();

        // Terminating variants
        product_fields.insert(
            "Scan".to_string(),
            vec![create_field("table_name", Type::String)],
        );

        product_fields.insert(
            "Literal".to_string(),
            vec![create_field("value", Type::Int64)],
        );

        product_fields.insert(
            "Equals".to_string(),
            vec![
                create_field("left", Type::Identifier("Scalar".to_string())),
                create_field("right", Type::Identifier("Scalar".to_string())),
            ],
        );

        // Recursive variants
        product_fields.insert(
            "PhysFilter".to_string(),
            vec![
                create_field("child", Type::Identifier("Physical".to_string())),
                create_field("cond", Type::Identifier("Predicate".to_string())),
            ],
        );

        product_fields.insert(
            "PhysProject".to_string(),
            vec![
                create_field("child", Type::Identifier("Physical".to_string())),
                create_field(
                    "exprs",
                    Type::Array(Spanned::new(
                        Type::Identifier("Scalar".to_string()),
                        create_test_span(),
                    )),
                ),
            ],
        );

        // PhysJoin variants
        product_fields.insert(
            "HashJoin".to_string(),
            vec![
                create_field("build_side", Type::Identifier("Physical".to_string())),
                create_field("probe_side", Type::Identifier("Physical".to_string())),
                create_field("typ", Type::String),
                create_field("cond", Type::Identifier("Predicate".to_string())),
            ],
        );

        product_fields.insert(
            "MergeJoin".to_string(),
            vec![
                create_field("left", Type::Identifier("Physical".to_string())),
                create_field("right", Type::Identifier("Physical".to_string())),
                create_field("typ", Type::String),
                create_field("cond", Type::Identifier("Predicate".to_string())),
            ],
        );

        product_fields.insert(
            "NestedLoopJoin".to_string(),
            vec![
                create_field("outer", Type::Identifier("Physical".to_string())),
                create_field("inner", Type::Identifier("Physical".to_string())),
                create_field("typ", Type::String),
                create_field("cond", Type::Identifier("Predicate".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok()); // Valid because Scan is a terminating variant
    }

    #[test]
    fn test_nested_list_with_cycles() {
        // This test represents the structure:
        //
        // type A =
        //  \ List =
        //  | Bla(b: List)
        //  \ Vla(a: List)

        // Set up type hierarchy
        let mut subtypes = BTreeMap::new();

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
        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Bla".to_string(),
            vec![create_field("b", Type::Identifier("List".to_string()))],
        );
        product_fields.insert(
            "Vla".to_string(),
            vec![create_field("a", Type::Identifier("List".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err()); // Should detect the cycle
    }

    #[test]
    fn test_complex_expression_language() {
        // Create a simplified expression language:
        //
        // type Expr =
        //   | Literal(value: Int64)
        //   | BinOp(left: Expr, right: Expr, op: String)
        //   | Condition(cond: Predicate)
        //
        // type Predicate =
        //   | Equals(left: Expr, right: Expr)
        //   | And(left: Predicate, right: Predicate)

        // Set up type h= BTreeMap::new();ierarchy
        let mut subtypes = BTreeMap::new();

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
        let mut product_fields = HashMap::new();

        // Terminating variant for Expr
        product_fields.insert(
            "Literal".to_string(),
            vec![create_field("value", Type::Int64)],
        );

        // Recursive variants
        product_fields.insert(
            "BinOp".to_string(),
            vec![
                create_field("left", Type::Identifier("Expr".to_string())),
                create_field("right", Type::Identifier("Expr".to_string())),
                create_field("op", Type::String),
            ],
        );

        product_fields.insert(
            "Condition".to_string(),
            vec![create_field(
                "cond",
                Type::Identifier("Predicate".to_string()),
            )],
        );

        product_fields.insert(
            "Equals".to_string(),
            vec![
                create_field("left", Type::Identifier("Expr".to_string())),
                create_field("right", Type::Identifier("Expr".to_string())),
            ],
        );

        product_fields.insert(
            "And".to_string(),
            vec![
                create_field("left", Type::Identifier("Predicate".to_string())),
                create_field("right", Type::Identifier("Predicate".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok()); // Valid because Literal is a terminating variant for Expr
    }

    #[test]
    fn test_nested_sum_only_recursive() {
        // Test a structure with nested sum types where all paths lead to recursion:
        //
        // type A =
        //   | B =
        //     | C(a: A)

        // Set up type hierarchy
        let mut subtypes = BTreeMap::new();

        // A has B as its only variant
        let mut a_variants = HashSet::new();
        a_variants.insert("B".to_string());
        subtypes.insert("A".to_string(), a_variants);

        // B has C as its only variant
        let mut b_variants = HashSet::new();
        b_variants.insert("C".to_string());
        subtypes.insert("B".to_string(), b_variants);

        // Set up fields
        let mut product_fields = HashMap::new();

        // C references back to A, creating a cycle
        product_fields.insert(
            "C".to_string(),
            vec![create_field("a", Type::Identifier("A".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err()); // Should detect the cycle and fail
    }

    #[test]
    fn test_duplicate_fields() {
        // Test a product type with duplicate field names:
        // type DuplicateField(x: Int64, x: String)

        let mut product_fields = HashMap::new();
        let fields = vec![
            create_field("x", Type::Int64),
            create_field("x", Type::String), // Duplicate field name
        ];

        product_fields.insert("DuplicateField".to_string(), fields);

        let mut subtypes = BTreeMap::new();
        subtypes.insert("DuplicateField".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_undefined_type_reference() {
        // Test a product type that references an undefined type:
        // type BadReference(field: UndefinedType)

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "BadReference".to_string(),
            vec![create_field(
                "field",
                Type::Identifier("UndefinedType".to_string()),
            )],
        );

        let mut subtypes = BTreeMap::new();
        subtypes.insert("BadReference".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_complex_type_with_duplicate_nested_fields() {
        // Test a more complex type with duplicate fields in nested structures
        // type Complex =
        //   | VariantA(x: Int64, y: String)
        //   | VariantB(x: Int64, x: Bool)  // Duplicate field in this variant

        let mut subtypes = BTreeMap::new();
        let mut complex_variants = HashSet::new();
        complex_variants.insert("VariantA".to_string());
        complex_variants.insert("VariantB".to_string());
        subtypes.insert("Complex".to_string(), complex_variants);

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "VariantA".to_string(),
            vec![
                create_field("x", Type::Int64),
                create_field("y", Type::String),
            ],
        );

        product_fields.insert(
            "VariantB".to_string(),
            vec![
                create_field("x", Type::Int64),
                create_field("x", Type::Bool), // Duplicate field
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }
}
